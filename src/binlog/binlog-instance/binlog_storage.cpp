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

#include "binlog_storage.h"

#include "fs_util.h"
#include "config.h"
#include "common_util.h"
#include "counter.h"
#include "env.h"
#include "binlog_converter.h"

using namespace oceanbase::logproxy;
namespace oceanbase::binlog {

int BinlogStorage::init()
{
  int file_index = 0;
  string binlog_dir = string(".") + BINLOG_DATA_DIR;

  // find mysql-bin.index
  BinlogIndexRecord record;
  g_index_manager->get_latest_index(record);
  std::string bin_path;
  std::string bin_name;
  if (record.get_file_name().empty()) {
    OMS_ATOMIC_INC(file_index);
    bin_name = binlog::CommonUtils::fill_binlog_file_name(file_index);
    bin_path = binlog_dir + bin_name;
    std::pair<std::string, uint64_t> current("", 0);
    std::pair<std::string, uint64_t> before("", 0);

    // If the gtid mapping is specified, we should start the binlog service from the specified gtid mapping
    if (!s_meta.binlog_config()->initial_ob_txn_id().empty() || s_meta.binlog_config()->initial_ob_txn_gtid_seq() > 0) {
      current.first = s_meta.binlog_config()->initial_ob_txn_id();
      current.second = current.first.empty() ? s_meta.binlog_config()->initial_ob_txn_gtid_seq() - 1
                                             : s_meta.binlog_config()->initial_ob_txn_gtid_seq();
    }
    BinlogIndexRecord index_record(bin_path, file_index);
    index_record.set_current_mapping(current);
    index_record.set_before_mapping(before);
    index_record.set_checkpoint(s_meta.cdc_config()->start_timestamp());
    index_record.set_position(0);
    g_index_manager->add_index(index_record);
  } else {
    bin_path = record.get_file_name();
  }
  this->_file_name = bin_path;
  OMS_INFO("[storage] Successfully init the current binlog file: {}", _file_name);
  return OMS_OK;
}

void BinlogStorage::run()
{
  std::vector<ObLogEvent*> events;
  events.reserve(s_config.read_wait_num.val());
  MsgBuf buffer;
  size_t buffer_pos = 0;
  BinlogIndexRecord index_record;
  g_index_manager->get_latest_index(index_record);
  if (index_record.get_file_name().empty()) {
    OMS_ERROR("Failed to get latest index record.");
    stop();
  }

  while (is_run()) {
    _stage_timer.reset();
    while (!_event_queue.poll(events, s_config.read_timeout_us.val()) || events.empty()) {
      if (!is_run()) {
        OMS_INFO("Binlog storage thread has been stopped.");
        break;
      }

      OMS_INFO("Empty log event queue put by convert thread, retry...");
    }

    if (storage_binlog_event(events, buffer, buffer_pos, index_record) != OMS_OK) {
      break;
    }

    finishing_touches(buffer, buffer_pos);
  }
  _converter.stop_converter();
}

int BinlogStorage::finishing_touches(MsgBuf& buffer, size_t& buffer_pos)
{
  buffer.reset();
  buffer_pos = 0;
  int64_t poll_us = _stage_timer.elapsed();
  Counter::instance().count_key(Counter::SENDER_SEND_US, poll_us);
  return OMS_OK;
}

int BinlogStorage::storage_binlog_event(
    vector<ObLogEvent*>& events, MsgBuf& buffer, size_t& buffer_pos, BinlogIndexRecord& index_record)
{
  Timer cache_time;
  cache_time.reset();
  for (auto event : events) {
    if (event == nullptr) {
      OMS_ERROR("event is an unexpected NULL value");
      continue;
    }
    OMS_DEBUG(event->str_format());

    if (event->get_header()->get_type_code() == HEARTBEAT_LOG_EVENT) {
      g_gtid_manager->mark_event(true, event->get_checkpoint(), index_record.get_current_mapping());
      g_gtid_manager->compress_and_save_gtid_seq(index_record.get_current_mapping());
      continue;
    }
    Counter::instance().count_write(1);
    _rate_limiter.in_event_with_alarm(event->get_header()->get_event_length());

    if (event->get_header()->get_type_code() == ROTATE_EVENT) {
      buffer_pos = rotate(event, buffer, buffer_pos, index_record);
      if (buffer_pos < 0) {
        OMS_ERROR("Failed to rotate, index record: {}", index_record.to_string());
        return OMS_FAILED;
      }
      continue;
    }

    if (event->get_header()->get_type_code() == GTID_LOG_EVENT) {
      _valid_events_coming = true;
      index_record.set_before_mapping(index_record.get_current_mapping());
      std::pair<std::string, int64_t> current_mapping(event->get_ob_txn(), ((GtidLogEvent*)event)->get_gtid_txn_id());
      index_record.set_current_mapping(current_mapping);
      g_gtid_manager->mark_event(false, event->get_checkpoint(), index_record.get_current_mapping());

      index_record.set_checkpoint(event->get_checkpoint() / 1000000);
      Counter::instance().mark_checkpoint(event->get_checkpoint());
      Counter::instance().mark_timestamp(
          ((GtidLogEvent*)event)->get_last_committed() * 1000000 + ((GtidLogEvent*)event)->get_sequence_number());
      OMS_DEBUG("current ob txn: {}, gtid txn id: {}, checkpoint: {}",
          event->get_ob_txn(),
          index_record.get_current_mapping().second,
          index_record.get_checkpoint());
    }

    auto* data = static_cast<unsigned char*>(malloc(event->get_header()->get_event_length()));
    size_t ret = event->flush_to_buff(data);

    if ((buffer_pos + event->get_header()->get_event_length()) >= s_config.binlog_max_event_buffer_bytes.val() ||
        (cache_time.elapsed() > s_config.binlog_convert_timeout_us.val() && buffer_pos != 0)) {
      if (FsUtil::append_file(_file_name, buffer) != OMS_OK) {
        return OMS_FAILED;
      }

      update_index_record(index_record);
      buffer.reset();
      Counter::instance().count_write_io(buffer_pos);
      buffer_pos = 0;
      cache_time.reset();
    }
    buffer.push_back(reinterpret_cast<char*>(data), ret);
    buffer_pos += ret;
  }

  release_vector(events);
  if (buffer_pos > 0) {
    if (FsUtil::append_file(_file_name, buffer) != OMS_OK) {
      return OMS_FAILED;
    }
    // update offset
    update_index_record(index_record);
    Counter::instance().count_write_io(buffer_pos);
  }
  return OMS_OK;
}

void BinlogStorage::stop()
{
  if (is_run()) {
    Thread::stop();
  }
  OMS_INFO("Begin to stop binlog storage thread...");
}

BinlogStorage::BinlogStorage(BinlogConverter& reader, BlockingQueue<ObLogEvent*>& event_queue)
    : _event_queue(event_queue), _converter(reader)
{
  _rate_limiter.update_throttle_rps(s_meta.binlog_config()->throttle_convert_rps());
  _rate_limiter.update_throttle_iops(s_meta.binlog_config()->throttle_convert_iops());
}

size_t init_binlog_file(MsgBuf& content, RotateEvent* rotate_event)
{
  auto* event_data = static_cast<unsigned char*>(malloc(1024));
  size_t buff_pos = 0;
  // add magic
  for (int i = 0; i < BINLOG_MAGIC_SIZE; ++i) {
    int1store(event_data + i, binlog_magic[i]);
  }
  buff_pos += BINLOG_MAGIC_SIZE;

  // add FormatDescriptionEvent
  FormatDescriptionEvent format_description_event = FormatDescriptionEvent(rotate_event->get_header()->get_timestamp());
  buff_pos += format_description_event.flush_to_buff(event_data + buff_pos);
  // add PreviousGtidsLogEvent
  std::vector<GtidMessage*> gtid_messages;
  rotate_event->get_next_previous_gtids(gtid_messages);
  PreviousGtidsLogEvent previous_gtids_log_event =
      PreviousGtidsLogEvent(gtid_messages.size(), gtid_messages, rotate_event->get_header()->get_timestamp());

  uint32_t next_pos = format_description_event.get_header()->get_next_position() +
                      previous_gtids_log_event.get_header()->get_event_length();

  previous_gtids_log_event.get_header()->set_next_position(next_pos);
  buff_pos += previous_gtids_log_event.flush_to_buff(event_data + buff_pos);
  content.push_back_copy(reinterpret_cast<char*>(event_data), buff_pos);
  OMS_INFO(
      "Successfully init binlog file(add FormatDescriptionEvent + PreviousGtidsLogEvent), current pos: {}", buff_pos);
  free(event_data);
  return buff_pos;
}

size_t BinlogStorage::rotate(ObLogEvent* event, MsgBuf& buffer, std::size_t size, BinlogIndexRecord& index_record)
{
  RotateEvent* rotate_event = ((RotateEvent*)event);
  if (rotate_event->get_op() != RotateEvent::INIT) {
    // purge expired binlogs according to BINLOG_EXPIRE_LOGS_SECONDS and BINLOG_EXPIRE_LOGS_SIZE
    purge_expired_binlog(index_record);

    if (rotate_current_binlog(buffer, size, rotate_event, index_record) != OMS_OK) {
      return OMS_FAILED;
    }

    if (OMS_OK != add_next_binlog_index(index_record, rotate_event)) {
      return OMS_FAILED;
    }
  }
  size_t buff_pos = init_binlog_file(buffer, rotate_event);
  return buff_pos;
}

int BinlogStorage::add_next_binlog_index(BinlogIndexRecord& index_record, const RotateEvent* rotate_event)
{
  const std::string& next_file = rotate_event->get_next_binlog_file_name();
  string bin_path = string(".") + BINLOG_DATA_DIR + next_file;
  index_record.set_index(rotate_event->get_index());
  index_record.set_file_name(bin_path);
  index_record.set_position(0);
  if (OMS_OK != g_index_manager->add_index(index_record)) {
    return OMS_FAILED;
  }

  _file_name = bin_path;
  OMS_INFO("Rotate to next binlog file: {}", next_file);
  return OMS_OK;
}

int BinlogStorage::rotate_current_binlog(
    MsgBuf& buffer, size_t size, RotateEvent* rotate_event, BinlogIndexRecord& index_record)
{
  if (!rotate_event->is_existed()) {
    // When the rotate event is not generated, the rotate event information should be supplemented
    auto* data = static_cast<unsigned char*>(malloc(rotate_event->get_header()->get_event_length()));
    size_t ret = rotate_event->flush_to_buff(data);
    buffer.push_back(reinterpret_cast<char*>(data), ret);
    size += ret;
    if (FsUtil::append_file(_file_name, buffer) != OMS_OK) {
      free(data);
      data = nullptr;
      return OMS_FAILED;
    }
    // update offset
    update_index_record(index_record);
    Counter::instance().count_write_io(buffer.byte_size());
    OMS_INFO("Rotate event: {}, offset: {}", index_record.get_file_name(), index_record.get_position());
    buffer.reset();
  }
  auto* flags = static_cast<unsigned char*>(malloc(FLAGS_LEN));
  int2store(flags, 0);
  FILE* fp = FsUtil::fopen_binary(_file_name, "rb+");
  FsUtil::rewrite(fp, flags, BINLOG_MAGIC_SIZE + FLAGS_OFFSET, FLAGS_LEN);
  free(flags);
  FsUtil::fclose_binary(fp);
  return OMS_OK;
}

void BinlogStorage::update_index_record(BinlogIndexRecord& index_record)
{
  _offset = FsUtil::file_size(_file_name);
  index_record.set_position(_offset);
  g_index_manager->update_index(index_record);
  g_gtid_manager->compress_and_save_gtid_seq(index_record.get_current_mapping());

  if (_valid_events_coming) {
    g_gtid_manager->update_gtid_executed(index_record.get_current_mapping().second);
  }
}

void BinlogStorage::purge_expired_binlog(BinlogIndexRecord& index_record)
{
  std::vector<BinlogIndexRecord*> index_records;
  g_index_manager->fetch_index_vector(index_records);
  defer(release_vector(index_records));

  OMS_INFO("Begin to check and clear expired binlog, total number of binlog: {}, binlog range: [{}, {}]",
      index_records.size(),
      index_records.front()->get_file_name(),
      index_records.back()->get_file_name());
  std::set<std::string> need_purged_binlog_files;
  uint64_t last_gtid_seq = 0;
  // 1. purge by binlog_expire_logs_seconds
  uint64_t now_s = Timer::now_s();
  OMS_INFO("Begin to purge binlog based on expiration time: {}, now: {}, and checkpoint range: [{}, {}]",
      s_meta.binlog_config()->binlog_expire_logs_seconds(),
      now_s,
      index_records.front()->get_checkpoint(),
      index_records.back()->get_checkpoint());
  if (s_meta.binlog_config()->binlog_expire_logs_seconds() > 0) {
    uint64_t minimum_checkpoint = index_record.get_checkpoint();

    for (auto iter = index_records.begin(); iter != index_records.end();) {
      if (index_record.equal_to(*(*iter))) {
        iter++;
        continue;
      }
      if (now_s - (*iter)->get_checkpoint() < s_meta.binlog_config()->binlog_expire_logs_seconds()) {
        minimum_checkpoint = (*iter)->get_checkpoint();
        break;
      }

      OMS_INFO("Binlog [{}] with last gtid [{}] exceeds binlog expiration time: {} - {} > {}",
          (*iter)->get_file_name(),
          (*iter)->get_current_mapping().second,
          now_s,
          (*iter)->get_checkpoint(),
          s_meta.binlog_config()->binlog_expire_logs_seconds());
      need_purged_binlog_files.insert((*iter)->get_file_name());
      if (last_gtid_seq < (*iter)->get_current_mapping().second) {
        last_gtid_seq = (*iter)->get_current_mapping().second;
      }
      delete *iter;
      iter = index_records.erase(iter);
    }
    OMS_INFO("Minimum binlog checkpoint after expiration purging: {}", minimum_checkpoint);
  } else {
    OMS_INFO("Automatically purge binlog files based on [expiration time] is turned off.");
  }

  // 2. purged by binlog_expire_logs_size
  OMS_INFO(
      "Begin to purge binlog based on expiration logs size: {}", s_meta.binlog_config()->binlog_expire_logs_size());
  uint64_t total_binlog_size = 0;
  if (s_meta.binlog_config()->binlog_expire_logs_size() > 0) {
    for (auto r_iter = index_records.rbegin(); r_iter != index_records.rend();) {
      if (index_record.equal_to(*(*r_iter))) {
        r_iter++;
        continue;
      }

      if (total_binlog_size < s_meta.binlog_config()->binlog_expire_logs_size()) {
        total_binlog_size += (*r_iter)->get_position();
        r_iter++;
      } else {
        OMS_INFO("Binlog [{}] with last gtid [{}] exceeds binlog expiration logs size: {} + {} > {}",
            (*r_iter)->get_file_name(),
            (*r_iter)->get_current_mapping().second,
            total_binlog_size,
            (*r_iter)->get_position(),
            s_meta.binlog_config()->binlog_expire_logs_size());
        need_purged_binlog_files.insert((*r_iter)->get_file_name());
        if (last_gtid_seq < (*r_iter)->get_current_mapping().second) {  // todo
          last_gtid_seq = (*r_iter)->get_current_mapping().second;
        }
        delete *r_iter;
        r_iter = std::vector<BinlogIndexRecord*>::reverse_iterator(index_records.erase((++r_iter).base()));
      }
    }
    OMS_INFO("Total binlog size after expiration purging: {}", total_binlog_size);
  } else {
    OMS_INFO("Automatically purge binlog files based on [expiration logs size] is turned off.");
  }

  // 3. purge tenant_gtid_seq.meta before last_gtid_seq
  if (0 != last_gtid_seq) {
    g_gtid_manager->purge_gtid_before(last_gtid_seq);
  }

  // 4. rewrite mysql-bin.index
  if (!need_purged_binlog_files.empty()) {
    std::string error_msg;
    if (OMS_OK != g_index_manager->rewrite_index_file(error_msg, index_records)) {
      OMS_ERROR("Failed to rewrite mysql-bin.index, error: {}", error_msg);
      return;
    }
  }

  // 5. remove binlog logs
  // todo check if binlog file is being consumed by dump connection
  for (const std::string& purge_file : need_purged_binlog_files) {
    std::error_code error_code;
    if (std::filesystem::remove(purge_file, error_code) && !error_code) {
      OMS_INFO("Successfully remove binlog log: {}.", purge_file);
    } else {
      OMS_ERROR("Failed to remove binlog log: {}", purge_file);
    }
  }
  OMS_INFO("Number of expired binlog cleaned: {}, and number of remaining binlog: {}, range: [{}, {}]",
      need_purged_binlog_files.size(),
      index_records.size(),
      index_records.front()->get_file_name(),
      index_records.back()->get_file_name());
}

}  // namespace oceanbase::binlog
