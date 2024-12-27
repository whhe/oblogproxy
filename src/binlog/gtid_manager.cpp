/**
 * Copyright (c) 2024 OceanBase
 * OceanBase Migration Service LogProxy is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "gtid_manager.h"

#include <algorithm>

#include "env.h"
#include "log.h"
#include "fs_util.h"
#include "str.h"

using namespace oceanbase::logproxy;
namespace oceanbase::binlog {

GtidSeq::GtidSeq(uint64_t commit_version_start, std::string xid_start, uint64_t gtid_start, uint64_t trxs_num)
    : _commit_version_start(commit_version_start),
      _xid_start(std::move(xid_start)),
      _gtid_start(gtid_start),
      _trxs_num(trxs_num)
{}

std::string GtidSeq::serialize() const
{
  return std::to_string(_commit_version_start) + "#" + _xid_start + "#" + std::to_string(_gtid_start) + "#" +
         std::to_string(_trxs_num);
}

void GtidSeq::deserialize(std::string line)
{
  std::vector<std::string> set;
  split(line, '#', set);
  if (set.size() == 4) {
    this->_commit_version_start = std::atoll(set.at(0).c_str());
    this->_xid_start = set.at(1);
    this->_gtid_start = std::atoll(set.at(2).c_str());
    this->_trxs_num = std::atoll(set.at(3).c_str());
  } else {
    OMS_WARN("Failed to deserialize gtid seq: {}, set size: {}", line, set.size());
  }
}

GtidManager::GtidManager(std::string gtid_seq_filename) : _gtid_seq_filename(std::move(gtid_seq_filename))
{}

GtidManager::~GtidManager()
{
  release_vector(_gtid_purged_set);
  release_vector(_gtid_executed_set);
}

int GtidManager::init()
{
  if (OMS_OK != init_gtid_variables()) {
    OMS_ERROR("Failed to init gtid variables.");
    return OMS_FAILED;
  }
  return OMS_OK;
}

void GtidManager::mark_event(
    bool is_heartbeat, uint64_t checkpoint, const std::pair<std::string, uint64_t>& trx_mapping)
{
  if (is_heartbeat) {
    _curr_hb_checkpoint = checkpoint / 1000000;
    return;
  }

  _nof_trx += 1;
  _curr_trx_checkpoint = checkpoint / 1000000;

  if (trx_mapping.second % s_config.gtid_marking_step_size.val() == 0) {
    GtidSeq gtid_seq_checkpoint(_curr_trx_checkpoint, trx_mapping.first, trx_mapping.second, 0);
    _gtid_checkpoint_set.emplace_back(gtid_seq_checkpoint);
  }
}

void GtidManager::compress_and_save_gtid_seq(const std::pair<std::string, uint64_t>& trx_mapping)
{
  // 1. save checkpoint gtid seq
  if (!_gtid_checkpoint_set.empty()) {
    for (const auto& gtid_seq : _gtid_checkpoint_set) {
      if (OMS_OK != save_gtid_seq(gtid_seq)) {
        OMS_ERROR("Failed to store checkpoint gtid seq: {}", gtid_seq.serialize());
      }
    }
    _gtid_checkpoint_set.clear();
  }

  // 2. compress gtid seq when exceed 10w transactions or 10s
  uint64_t max_curr_checkpoint = std::max(_curr_hb_checkpoint, _curr_trx_checkpoint);
  if (max_curr_checkpoint <= _last_saved_commit_version_high) {
    return;
  }

  if ((max_curr_checkpoint - _last_saved_commit_version_high >= s_config.gtid_seq_compressed_interval_s.val()) ||
      (max_curr_checkpoint > _last_saved_commit_version_high &&
          _nof_trx >= s_config.gtid_seq_compressed_trx_size.val())) {
    uint64_t commit_version_start = _nof_trx > 0 ? _curr_trx_checkpoint : _curr_hb_checkpoint;
    std::string xid_start = _nof_trx > 0 ? trx_mapping.first : "";
    GtidSeq gtid_seq(commit_version_start, xid_start, trx_mapping.second, _nof_trx);
    if (OMS_OK != save_gtid_seq(gtid_seq)) {
      OMS_ERROR("Failed to store gtid seq: {}", gtid_seq.serialize());
    }
    _last_saved_commit_version_high = max_curr_checkpoint;
    _nof_trx = 0;

    // 3. clear heartbeat gtid seqs that exceed BINLOG_EXPIRE_LOGS_SECONDS
    clear_outdated_heartbeat_gtid_seqs_periodically(max_curr_checkpoint);
  }
}

int GtidManager::save_gtid_seq(const GtidSeq& gtid_seq)
{
  std::lock_guard<std::timed_mutex> op_lock(_op_mutex);
  // 0. keep monotonically increasing when restarted
  if (first_entry_after_restarted && !_gtid_seq_set.empty()) {
    GtidSeq last_gtid_seq;
    last_gtid_seq.deserialize(_gtid_seq_set.back().second);
    if (gtid_seq.get_commit_version_start() <= last_gtid_seq.get_commit_version_start()) {
      OMS_INFO("Skipped gtid seq({}), which is earlier than the last gitd seq: {}",
          gtid_seq.serialize(),
          _gtid_seq_set.back().second);
      return OMS_OK;
    }
  }

  std::string gtid_seq_str = gtid_seq.serialize();
  // 1. append to memory
  uint64_t curr_checkpoint = gtid_seq.get_commit_version_start();
  for (auto iter = _gtid_seq_set.begin(); iter != _gtid_seq_set.end();) {
    if ((curr_checkpoint - (*iter).first) < s_config.gtid_memory_cache_seconds.val()) {
      break;
    }
    iter = _gtid_seq_set.erase(iter);
  }
  _gtid_seq_set.emplace_back(gtid_seq.get_commit_version_start(), gtid_seq_str);

  // 2. append to file
  std::string gtid_seq_line(gtid_seq_str + "\n");
  int64_t ret = FsUtil::append_file(_gtid_seq_filename, gtid_seq_line);
  if (OMS_FAILED == ret) {
    OMS_ERROR(
        "Failed to store gtid seq [{}] to file {}, error: {}.", gtid_seq_str, _gtid_seq_filename, system_err(errno));
    return OMS_FAILED;
  }

  first_entry_after_restarted = false;
  OMS_DEBUG("Stored gtid seq to file file: {}, value: {}", _gtid_seq_filename, gtid_seq_str);
  return OMS_OK;
}

void GtidManager::purge_gtid_before(uint64_t last_gtid_purged)
{
  std::lock_guard<std::timed_mutex> op_lock(_op_mutex);
  // 1. purge gtid_seq_sets and rewrite file
  OMS_INFO("Begin to purge gtid seq before {}", last_gtid_purged);
  rewrite_gtid_file([&](const GtidSeq& gtid_seq) { return gtid_seq.get_gtid_start() > last_gtid_purged; }, true);

  // 2. update gtid_purged according to gtid_seq
  update_gtid_purged(last_gtid_purged);
}

void GtidManager::update_gtid_purged(uint64_t gtid)
{
  _last_gtid_purged = gtid;
  merge_gtid_before(_gtid_purged_set, txn_range(1, gtid));
  std::string gtid_purged_str = join_vector_str(_gtid_purged_set, gtid_str_generator);
  g_sys_var->update_global_var("gtid_purged", gtid_purged_str);
}

void GtidManager::update_gtid_executed(uint64_t gtid)
{
  merge_gtid_before(_gtid_executed_set, txn_range(1, gtid));
  std::string gtid_purged_str = join_vector_str(_gtid_executed_set, gtid_str_generator);
  g_sys_var->update_global_var("gtid_executed", gtid_purged_str);
}

void GtidManager::get_instance_incr_gtids_string(uint64_t timestamp, std::string& gtids)
{
  if (!_op_mutex.try_lock_for(std::chrono::seconds(5))) {
    OMS_ERROR("Try to obtain lock timeout.");
  } else {
    defer(_op_mutex.unlock());
    if (_gtid_seq_set.empty()) {
      return;
    }

    uint32_t count = 0;
    if (timestamp >= _gtid_seq_set.front().first) {
      for (const auto& gtid_seq : _gtid_seq_set) {
        if (gtid_seq.first > timestamp) {
          gtids.append(gtid_seq.second).append("|");
          count += 1;
        }
        if (count > s_config.max_incr_gtid_seqs_num.val()) {
          OMS_INFO("Exceed max number of gtid seqs to report, current checkpoint [{}], report checkpoint [{}]",
              gtid_seq.first,
              timestamp);
          break;
        }
      }
    } else {
      OMS_INFO("The minimum commit version cached in memory is [{}], and get gtid seq greater than [{}] from file",
          _gtid_seq_set.front().first,
          timestamp);
      std::ifstream ifs(_gtid_seq_filename);
      if (!ifs.good()) {
        OMS_ERROR("Failed to open gtid seq file: {}, error: {}", _gtid_seq_filename, system_err(errno));
        return;
      }

      for (std::string line; std::getline(ifs, line);) {
        GtidSeq gtid_seq;
        gtid_seq.deserialize(line);
        if (gtid_seq.get_commit_version_start() > timestamp) {
          gtids.append(line).append("|");
          count += 1;
        }
        if (count > s_config.max_incr_gtid_seqs_num.val()) {
          OMS_INFO("Exceed max number of gtid seqs to report, current checkpoint [{}], report checkpoint [{}]",
              gtid_seq.get_commit_version_start(),
              timestamp);
          break;
        }
      }
    }

    if (!gtids.empty()) {
      gtids.pop_back();
    }
  }
}

int GtidManager::get_latest_gtid_seq(GtidSeq& gtid_seq)
{
  std::lock_guard<std::timed_mutex> op_lock(_op_mutex);
  if (_gtid_seq_set.empty()) {
    return OMS_FAILED;
  }

  gtid_seq.deserialize(_gtid_seq_set.back().second);
  return OMS_OK;
}

int GtidManager::get_first_le_gtid_seq(uint64_t gtid, GtidSeq& gtid_seq)
{
  std::lock_guard<std::timed_mutex> op_lock(_op_mutex);
  for (auto rit = _gtid_seq_set.rbegin(); rit != _gtid_seq_set.rend(); ++rit) {
    GtidSeq local_gtid_seq;
    local_gtid_seq.deserialize(rit->second);
    if (local_gtid_seq.get_gtid_start() > gtid) {
      continue;
    }

    if ((local_gtid_seq.get_gtid_start() < gtid) ||
        (local_gtid_seq.get_gtid_start() == gtid && !local_gtid_seq.get_xid_start().empty())) {
      gtid_seq = local_gtid_seq;
      return OMS_OK;
    }
  }

  return OMS_FAILED;
}

void GtidManager::get_last_gtid_purged(uint64_t& last_gtid_purged) const
{
  last_gtid_purged = _last_gtid_purged;
}

void GtidManager::init_gtid_seq()
{
  GtidSeq last_gtid_seq;
  read_last_valid_gtid_seq(last_gtid_seq);
  uint64_t last_checkpoint =
      last_gtid_seq.get_commit_version_start() == 0 ? Timer::now_s() : last_gtid_seq.get_commit_version_start();

  std::ifstream ifs(_gtid_seq_filename);
  if (!ifs.good()) {
    OMS_INFO("Failed to open gtid seq file: {}, error: {}", _gtid_seq_filename, system_err(errno));
    return;
  }

  GtidSeq gtid_seq;
  uint64_t total = 0;
  for (std::string line; std::getline(ifs, line);) {
    gtid_seq.deserialize(line);
    _last_checkpoint_purged =
        (_last_checkpoint_purged == 0) ? gtid_seq.get_commit_version_start() : _last_checkpoint_purged;
    if (last_checkpoint - gtid_seq.get_commit_version_start() < s_config.gtid_memory_cache_seconds.val()) {
      _gtid_seq_set.emplace_back(gtid_seq.get_commit_version_start(), line);
    }
    total += 1;
  }

  first_entry_after_restarted = true;
  OMS_INFO("Successfully inited from the existing tenant gtid seq, total: {}, cached: {}, max gtid: {}, max "
           "checkpoint: {}={}, last checkpoint purged: {}",
      total,
      _gtid_seq_set.size(),
      gtid_seq.get_gtid_start(),
      last_checkpoint,
      gtid_seq.get_commit_version_start(),
      _last_checkpoint_purged);
}

int get_previous_gtid_set(const std::string& binlog_file, std::vector<GtidMessage*>& gtid_set)
{
  std::vector<ObLogEvent*> log_events;
  defer(release_vector(log_events));
  if (OMS_OK != seek_events(binlog_file, log_events, EventType::PREVIOUS_GTIDS_LOG_EVENT, true)) {
    OMS_INFO("Failed to get previous gtid event from binlog: {}", binlog_file);
    return OMS_FAILED;
  }
  assert(log_events.size() == 1);

  auto* previous_gtids_log_event = dynamic_cast<PreviousGtidsLogEvent*>(log_events.at(0));
  std::map<std::string, GtidMessage*> gtids = previous_gtids_log_event->get_gtid_messages();
  for (const auto& gtid_pair : gtids) {
    auto* gtid_msg = new GtidMessage();
    gtid_msg->copy_from(*gtid_pair.second);
    gtid_set.emplace_back(gtid_msg);
  }
  return OMS_OK;
}

int GtidManager::init_gtid_variables()
{
  std::vector<BinlogIndexRecord*> index_records;
  g_index_manager->fetch_index_vector(index_records);
  defer(release_vector(index_records));

  if (index_records.empty() && s_meta.binlog_config()->initial_ob_txn_gtid_seq() > 1) {
    merge_gtid_before(_gtid_purged_set, txn_range(1, s_meta.binlog_config()->initial_ob_txn_gtid_seq() - 1));
    merge_gtid_before(_gtid_executed_set, txn_range(1, s_meta.binlog_config()->initial_ob_txn_gtid_seq() - 1));
  } else if (!index_records.empty()) {
    BinlogIndexRecord first_index_record = *(index_records.front());
    if (OMS_OK != get_previous_gtid_set(first_index_record.get_file_name(), _gtid_purged_set)) {
      return OMS_FAILED;
    }

    BinlogIndexRecord last_index_record = *(index_records.back());
    if (OMS_OK != get_previous_gtid_set(last_index_record.get_file_name(), _gtid_executed_set)) {
      return OMS_FAILED;
    }

    std::vector<ObLogEvent*> gtid_los_events;
    defer(release_vector(gtid_los_events));
    if (OMS_OK != seek_events(last_index_record.get_file_name(), gtid_los_events, EventType::GTID_LOG_EVENT, false)) {
      OMS_INFO("Failed to get gtid log event from last binlog: {}", last_index_record.get_file_name());
      return OMS_FAILED;
    }
    uint64_t last_gtid = 0;
    if (!gtid_los_events.empty()) {
      auto* last_gtid_log_event = dynamic_cast<GtidLogEvent*>(gtid_los_events.back());
      last_gtid = last_gtid_log_event->get_gtid_txn_id();
    } else if (last_index_record.get_current_mapping().second > 0) {
      last_gtid = last_index_record.get_before_mapping().first.empty()
                      ? last_index_record.get_current_mapping().second - 1
                      : last_index_record.get_current_mapping().second;
    }
    if (last_gtid > 0) {
      merge_gtid_before(_gtid_executed_set, txn_range(1, last_gtid));
    }
  }

  std::string gtid_purged_str = join_vector_str(_gtid_purged_set, gtid_str_generator);
  std::string gtid_executed_str = join_vector_str(_gtid_executed_set, gtid_str_generator);
  g_sys_var->update_global_var("gtid_purged", gtid_purged_str);
  g_sys_var->update_global_var("gtid_executed", gtid_executed_str);
  OMS_INFO("Init global variables [gtid_purged = {}, gtid_executed = {}] when first startup: {} ",
      gtid_purged_str,
      gtid_executed_str,
      index_records.empty());
  return OMS_OK;
}

void GtidManager::init_start_timestamp(uint64_t start_timestamp)
{
  _last_saved_commit_version_high = start_timestamp;
}

void GtidManager::clear_outdated_heartbeat_gtid_seqs_periodically(uint64_t current_checkpoint)
{
  int elapsed = current_checkpoint - _last_checkpoint_purged;
  if (elapsed > (int)(1.2 * (double)s_meta.binlog_config()->binlog_expire_logs_seconds())) {
    std::lock_guard<std::timed_mutex> op_lock(_op_mutex);
    _last_checkpoint_purged = current_checkpoint - s_meta.binlog_config()->binlog_expire_logs_seconds();
    OMS_INFO("Begin to purge outdated heartbeat gtid seq before checkpoint: {}", _last_checkpoint_purged);

    rewrite_gtid_file([&](GtidSeq gtid_seq) {
      return gtid_seq.get_commit_version_start() >= _last_checkpoint_purged ||
             (gtid_seq.get_commit_version_start() < _last_checkpoint_purged && !gtid_seq.get_xid_start().empty());
    });
  }
}

void GtidManager::rewrite_gtid_file(const std::function<bool(GtidSeq&)>& compare_func, bool update_first_checkpoint)
{
  // 1. filter out valid gtid seqs
  std::string removing_gtid_seq_filename = _gtid_seq_filename + ".removing";
  FILE* fp = FsUtil::fopen_binary(removing_gtid_seq_filename, "w+");
  defer(FsUtil::fclose_binary(fp));

  std::ifstream ifs(_gtid_seq_filename);
  if (!ifs.good()) {
    OMS_ERROR("Failed to open gtid seq file: {}, error: {}", _gtid_seq_filename, system_err(errno));
    return;
  }

  uint64_t first_checkpoint = 0;
  for (std::string line; std::getline(ifs, line);) {
    GtidSeq gtid_seq;
    gtid_seq.deserialize(line);
    if (compare_func(gtid_seq)) {
      first_checkpoint = (first_checkpoint == 0) ? gtid_seq.get_commit_version_start() : first_checkpoint;
      std::string gtid_seq_line = line + "\n";
      FsUtil::append_file(fp, gtid_seq_line, gtid_seq_line.size());
    }
  }

  // 2. rename
  std::error_code err;
  fs::rename(removing_gtid_seq_filename, _gtid_seq_filename, err);
  if (err) {
    OMS_ERROR(
        "Failed to rename file: {} -> {}, error: {}", removing_gtid_seq_filename, _gtid_seq_filename, err.message());
  }
  _last_checkpoint_purged = update_first_checkpoint ? first_checkpoint : _last_checkpoint_purged;
}

void GtidManager::get_last_checkpoint_purged(uint64_t& last_checkpoint_purged) const
{
  last_checkpoint_purged = _last_checkpoint_purged;
}

void GtidManager::read_last_valid_gtid_seq(GtidSeq& gtid_seq)
{
  std::ifstream file(_gtid_seq_filename, std::ios::binary | std::ios::ate);
  if (!file.is_open()) {
    OMS_ERROR("Failed to open gtid seq file: {}, error: {}", _gtid_seq_filename, system_err(errno));
    return;
  }

  uint64_t file_size = file.tellg();
  uint64_t position = file_size - 1;
  while (file_size > 0 && gtid_seq.get_commit_version_start() == 0) {
    char ch;
    for (; position > 0; --position) {
      file.seekg(position);
      file.get(ch);
      if (ch == '\n' && position != file_size - 1) {
        break;
      }
    }

    if (position == 0) {
      file.seekg(0);
    } else {
      file.seekg(position + 1);
    }
    std::string content;
    std::getline(file, content);
    gtid_seq.deserialize(content);

    file_size = position;
    position = file_size - 1;
  }
}

int binary_search_gtid_seq(uint64_t timestamp, const std::string& tenant_gtids_str, GtidSeq& gtid_seq)
{
  if (tenant_gtids_str.empty()) {
    OMS_ERROR("The given tenant_gtids_str is empty.");
    return OMS_FAILED;
  }

  std::vector<std::string> gtid_seq_set;
  split(tenant_gtids_str, '|', gtid_seq_set);

  OMS_INFO("Binary search gtid seq for given timestamp: {}, minimum gtid seq: {}, maximum gtid seq: {}",
      timestamp,
      gtid_seq_set.front(),
      gtid_seq_set.back());
  // 1. boundary value
  GtidSeq local_gtid_seq;
  local_gtid_seq.deserialize(gtid_seq_set.back());
  if (0 == timestamp || timestamp > local_gtid_seq.get_commit_version_start()) {
    OMS_INFO("The timestamp({}) is not specified or exceeds the maximum tenant_gtid_seq, so returned the "
             "maximum gitd seq: {}",
        timestamp,
        gtid_seq_set.back());
    gtid_seq.deserialize(gtid_seq_set.back());
    return OMS_OK;
  }

  local_gtid_seq.deserialize(gtid_seq_set.front());
  if (timestamp < local_gtid_seq.get_commit_version_start()) {
    OMS_INFO("The given commit version({}) is smaller than the minimum tenant_gtid_seq, so returned it: {}",
        timestamp,
        gtid_seq_set.front());
    gtid_seq.deserialize(gtid_seq_set.front());
    return OMS_OK;
  }

  // 2. binary search
  uint64_t idx_low = 0, idx_high = gtid_seq_set.size() - 1, idx_mid;
  while (idx_low <= idx_high) {
    idx_mid = idx_low + ((idx_high - idx_low) >> 1);
    local_gtid_seq.deserialize(gtid_seq_set[idx_mid]);
    if (local_gtid_seq.get_commit_version_start() > timestamp) {
      idx_high = idx_mid - 1;
    } else {
      idx_low = idx_mid + 1;
    }
  }

  OMS_INFO("For timestamp [{}], return gtid seq: {}", timestamp, gtid_seq_set[idx_high]);
  gtid_seq.deserialize(gtid_seq_set[idx_high]);
  return OMS_OK;
}

int boundary_gtid_seq(
    const std::string& tenant_gtids_str, std::pair<GtidSeq, GtidSeq>& gtid_seq_pair, std::string instance)
{
  if (tenant_gtids_str.empty()) {
    OMS_WARN("Empty gtid seq string.");
    return OMS_FAILED;
  }

  size_t first_split_pos = tenant_gtids_str.find('|');
  std::string minimum_gtid_str;
  std::string maximum_gtid_str;
  if (first_split_pos == std::string::npos) {
    minimum_gtid_str = tenant_gtids_str;
    maximum_gtid_str = tenant_gtids_str;
  } else {
    minimum_gtid_str = tenant_gtids_str.substr(0, first_split_pos);
    size_t last_split_pos = tenant_gtids_str.rfind('|');
    maximum_gtid_str = tenant_gtids_str.substr(last_split_pos + 1, tenant_gtids_str.size());
  }

  gtid_seq_pair.first.deserialize(minimum_gtid_str);
  gtid_seq_pair.second.deserialize(maximum_gtid_str);
  OMS_INFO("Instance [{}] boundary gtid seq: [{}, {}]", instance, minimum_gtid_str, maximum_gtid_str);
  return OMS_OK;
}

void merge_gtid_before(std::vector<GtidMessage*>& gtid_message_set, txn_range gtid_range)
{
  std::string server_uuid = s_meta.binlog_config()->master_server_uuid();
  bool uuid_existed = false;
  if (!gtid_message_set.empty()) {
    for (auto& gtid_message : gtid_message_set) {
      if (strcasecmp(gtid_message->get_gtid_uuid_str_wtih_delimiter().c_str(), server_uuid.c_str()) == 0) {
        if (!gtid_message->get_txn_range().empty()) {
          txn_range last_range = gtid_message->get_txn_range().back();
          if (gtid_range.first > (last_range.second + 1)) {
            gtid_message->get_txn_range().emplace_back(gtid_range);
          } else if (gtid_range.second > last_range.second) {
            gtid_message->get_txn_range().back().second = gtid_range.second;
          }
        } else {
          gtid_message->get_txn_range().emplace_back(gtid_range);
        }

        gtid_message->set_gtid_txn_id_intervals(gtid_message->get_txn_range().size());
        uuid_existed = true;
      }
    }
  }

  if (!uuid_existed) {
    if (gtid_range.first > 0 && gtid_range.second > 0) {
      auto* gtid_message = new GtidMessage();
      gtid_message->set_gtid_uuid(server_uuid);
      gtid_message->get_txn_range().emplace_back(gtid_range);
      gtid_message->set_gtid_txn_id_intervals(gtid_message->get_txn_range().size());
      gtid_message_set.emplace_back(gtid_message);
    }
  }
}
}  // namespace oceanbase::binlog
