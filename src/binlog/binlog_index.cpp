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

#include "binlog_index.h"

#include <iterator>
#include <utility>

#include "env.h"
#include "log.h"
#include "str.h"
#include "common_util.h"
#include "data_type.h"
#include "guard.hpp"
#include "file_lock.h"

namespace oceanbase::binlog {

namespace fs = std::filesystem;
using namespace oceanbase::logproxy;

BinlogIndexManager::BinlogIndexManager(std::string index_filename) : _index_filename(std::move(index_filename))
{}

void parse_index_record(FILE* fp, BinlogIndexRecord& record)
{
  char* line = nullptr;
  size_t len = 0;
  getline(&line, &len, fp);
  record.parse(line);
  free(line);
}

int BinlogIndexManager::fetch_index_vector(std::vector<BinlogIndexRecord*>& index_records)
{
  std::lock_guard<std::recursive_mutex> op_lock(_op_mutex);
  FILE* fp = FsUtil::fopen_binary(_index_filename, "r+");
  defer(FsUtil::fclose_binary(fp));
  if (nullptr == fp) {
    OMS_ERROR("Failed to open index file: {}, errno: {}", _index_filename, logproxy::system_err(errno));
    return OMS_FAILED;
  }

  char* buffer = nullptr;
  size_t buf_size = 0;
  while (getline(&buffer, &buf_size, fp) != -1) {
    auto* record = new BinlogIndexRecord();
    record->parse(buffer);
    if (record->get_index() > 0) {
      index_records.emplace_back(record);
    } else {
      delete (record);
    }
    free(buffer);
    buffer = nullptr;
  }
  if (nullptr != buffer) {
    free(buffer);
  }
  return OMS_OK;
}

int BinlogIndexManager::get_index(const std::string& binlog_file, BinlogIndexRecord& record)
{
  std::lock_guard<std::recursive_mutex> op_lock(_op_mutex);
  // close will be called automatically when ifs is destroyed
  std::ifstream ifs(_index_filename);
  if (!ifs.good()) {
    OMS_ERROR("Failed to open index file: {}, errno: {}", _index_filename, logproxy::system_err(errno));
    return OMS_FAILED;
  }

  for (std::string line; std::getline(ifs, line);) {
    BinlogIndexRecord index_record;
    index_record.parse(line);
    if (index_record.get_index() > 0 &&
        strcmp(binlog_file.c_str(), binlog::CommonUtils::fill_binlog_file_name(index_record.get_index()).c_str()) ==
            0) {
      record = index_record;
      return OMS_OK;
    }
  }
  OMS_ERROR("Failed to find binlog file {} in index file: {}", binlog_file, _index_filename);
  return OMS_FAILED;
}

int BinlogIndexManager::get_first_index(BinlogIndexRecord& record)
{
  std::lock_guard<std::recursive_mutex> op_lock(_op_mutex);
  // close will be called automatically when ifs is destroyed
  std::ifstream ifs(_index_filename);
  if (!ifs.good()) {
    OMS_ERROR("Failed to open index file: {}, errno: {}", _index_filename, logproxy::system_err(errno));
    return OMS_FAILED;
  }

  std::string line;
  std::getline(ifs, line);
  record.parse(line);
  return OMS_OK;
}

int BinlogIndexManager::get_latest_index(BinlogIndexRecord& record, size_t index)
{
  std::lock_guard<std::recursive_mutex> op_lock(_op_mutex);
  FILE* fp = FsUtil::fopen_binary(_index_filename, "r+");
  defer(FsUtil::fclose_binary(fp));
  if (nullptr == fp) {
    OMS_ERROR("Failed to open binlog index file: {}, error: {}", _index_filename, logproxy::system_err(errno));
    return OMS_FAILED;
  }

  FsUtil::seekg_last_line(fp, index);
  parse_index_record(fp, record);
  OMS_DEBUG("get binlog index file: {}, value: {}", record.get_file_name(), record.to_string());
  return OMS_OK;
}

int BinlogIndexManager::add_index(const BinlogIndexRecord& record)
{
  std::unique_lock<std::recursive_mutex> op_lock(_op_mutex);
  std::string record_str = record.to_string();
  int64_t ret = FsUtil::append_file(_index_filename, record_str);
  if (OMS_FAILED == ret) {
    OMS_ERROR("Failed to add index: [{}] to index file {}", record_str, _index_filename);
    return OMS_FAILED;
  }

  OMS_INFO("Add binlog index file: {}, value: {}", record.get_file_name(), record_str);
  return OMS_OK;
}

bool BinlogIndexManager::is_active(const std::string& file)
{
  std::lock_guard<std::recursive_mutex> op_lock(_op_mutex);
  return _memory_index_record.get_file_name().empty() || _memory_index_record.get_file_name() == file;
}

void BinlogIndexManager::get_memory_index(BinlogIndexRecord& record)
{
  std::lock_guard<std::recursive_mutex> op_lock(_op_mutex);
  if (_memory_index_record.get_file_name().empty()) {
    get_latest_index(_memory_index_record);
  }

  record = _memory_index_record;
}

bool BinlogIndexManager::is_behind_current_pos(const std::string& file, uint64_t pos, uint64_t wait_time_us)
{
  std::unique_lock<std::recursive_mutex> op_lock(_op_mutex);
  uint64_t file_index = CommonUtils::get_binlog_index(file);
  return _new_events.wait_for(op_lock, std::chrono::microseconds(wait_time_us), [&] {
    return (_memory_index_record.get_index() > file_index) ||
           (_memory_index_record.get_index() == file_index && _memory_index_record.get_position() > pos);
  });
}

int BinlogIndexManager::update_index(const BinlogIndexRecord& record, size_t pos)
{
  std::unique_lock<std::recursive_mutex> op_lock(_op_mutex);

  // 1. copy original index file to tmp file
  std::string temp_index_file = _index_filename + ".tmp";
  std::error_code err;
  fs::copy(_index_filename, temp_index_file, fs::copy_options::overwrite_existing, err);
  if (err) {
    OMS_ERROR("Failed to copy file: [{}] to [{}], error: {}", _index_filename, temp_index_file, err.message());
    return OMS_FAILED;
  }

  // 2. truncate last line
  FILE* fp = FsUtil::fopen_binary(temp_index_file, "r+");
  defer(FsUtil::fclose_binary(fp));
  if (nullptr == fp) {
    OMS_ERROR("Failed to open file: {}, error: {}", temp_index_file, logproxy::system_err(errno));
    return OMS_FAILED;
  }

  FsUtil::seekg_last_line(fp, pos);
  fs::resize_file(temp_index_file, pos, err);
  if (err) {
    OMS_ERROR("Failed to resize file:{} range[{},{}], error: {}", temp_index_file, 0, pos, logproxy::system_err(errno));
    return OMS_FAILED;
  }

  // 3. rewrite last line
  std::string str = record.to_string();
  FsUtil::rewrite(fp, (unsigned char*)str.c_str(), pos, str.size());
  fs::rename(temp_index_file, _index_filename, err);
  if (err) {
    OMS_ERROR(
        "Failed to rename file: {} to {}, error: {}", temp_index_file, temp_index_file, logproxy::system_err(errno));
    return OMS_FAILED;
  }

  _memory_index_record = record;
  _new_events.notify_all();
  return OMS_OK;
}

int BinlogIndexManager::remove_binlog(const BinlogIndexRecord& index_record)
{
  std::lock_guard<std::recursive_mutex> op_lock(_op_mutex);
  // close will be called automatically when ifs is destroyed
  std::ifstream ifs(_index_filename);
  if (!ifs.good()) {
    OMS_ERROR("Failed to open index file: {}, errno: {}", _index_filename, logproxy::system_err(errno));
    return OMS_FAILED;
  }

  // 1. remove index record from mysql-bin.index
  bool is_found = false;
  std::vector<BinlogIndexRecord*> index_records;
  defer(release_vector(index_records));
  for (std::string line; std::getline(ifs, line);) {
    auto* record = new BinlogIndexRecord();
    record->parse(line);
    if (!record->equal_to(index_record)) {
      index_records.emplace_back(record);
    } else {
      is_found = true;
      delete record;
    }
  }

  // 2. verify
  if (!is_found) {
    OMS_WARN("Not found the binlog file {} to remove", index_record.get_file_name());
    return OMS_OK;
  }

  // 3. rewrite mysql-bin.index and delete binlog file
  if (index_records.empty()) {
    OMS_WARN("Removed the only one binlog file: {}", index_record.get_file_name());
  }
  std::string err_msg;
  if (OMS_OK != rewrite_index_file(err_msg, index_records)) {
    return OMS_FAILED;
  }

  std::error_code err;
  fs::remove(index_record.get_file_name(), err);
  if (err) {
    OMS_ERROR("Failed to remove binlog file: {}, error: {}", index_record.get_file_name(), err.message());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int BinlogIndexManager::purge_binlog_index(const std::string& base_name, const std::string& binlog_file,
    const std::string& before_purge_ts, std::string& error_msg, std::vector<std::string>& purge_binlog_files)
{
  std::lock_guard<std::recursive_mutex> op_lock(_op_mutex);
  // 1. get all index records
  std::vector<BinlogIndexRecord*> index_records;
  int ret = fetch_index_vector(index_records);
  defer(release_vector(index_records));
  if (OMS_OK != ret) {
    OMS_ERROR("Failed to open file: {}", _index_filename);
    return OMS_FAILED;
  }

  // 2. purge binlog file in mysql-bin.index
  uint64_t last_gtid_seq = 0;
  if (binlog_file.empty()) {
    ret = purge_binlog_before_ts(before_purge_ts, index_records, error_msg, purge_binlog_files, last_gtid_seq);
  } else {
    ret = purge_binlog_to_file(base_name + binlog_file, error_msg, index_records, purge_binlog_files, last_gtid_seq);
  }

  // 3. purge tenant_gtid_seq.meta before
  if (0 != last_gtid_seq) {
    g_gtid_manager->purge_gtid_before(last_gtid_seq);
  }

  // 4. rewrite mysql-bin.index
  if (ret == OMS_OK && !purge_binlog_files.empty() && !index_records.empty()) {
    ret = rewrite_index_file(error_msg, index_records);
  }
  return ret;
}

uint64_t convert_ts(const std::string& before_purge_ts)
{
  IDate date = str_2_idate(before_purge_ts);
  tm tt{};
  tt.tm_year = date.year - 1900;
  tt.tm_mon = date.month - 1;
  tt.tm_mday = date.day;
  tt.tm_hour = date.hour;
  tt.tm_min = date.minute;
  tt.tm_sec = date.second;
  tt.tm_isdst = 0;
  auto time_c = mktime(&tt);
  uint64_t purge_ts = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::from_time_t(time_c).time_since_epoch())
                          .count();
  return purge_ts;
}

int BinlogIndexManager::purge_binlog_before_ts(const std::string& before_purge_ts,
    std::vector<BinlogIndexRecord*>& index_records, std::string& error_msg,
    std::vector<std::string>& purge_binlog_files, uint64_t& last_gtid_seq)
{
  if (before_purge_ts.empty()) {
    error_msg = "The specified cleanup time cannot be empty";
    OMS_ERROR(error_msg);
    return OMS_FAILED;
  }

  // Convert the clean binlog log time to time stamp without time zone
  uint64_t purge_ts = convert_ts(before_purge_ts);
  OMS_INFO("Begin to purge binlog before timestamp: {}({})", before_purge_ts, purge_ts);
  if (!index_records.empty()) {
    for (auto it = index_records.begin(); it != index_records.end();) {
      // The binlog file being written cannot be deleted
      if ((*it)->get_checkpoint() < purge_ts && !is_active((*it)->get_file_name())) {
        // purge
        purge_binlog_files.emplace_back((*it)->get_file_name());
        last_gtid_seq = (*it)->get_current_mapping().second;

        delete *it;
        it = index_records.erase(it);
      } else {
        ++it;
      }
    }
  }
  return OMS_OK;
}

int BinlogIndexManager::purge_binlog_to_file(const std::string& to_binlog_index, std::string& error_msg,
    std::vector<BinlogIndexRecord*>& index_records, std::vector<std::string>& purge_binlog_files,
    uint64_t& last_gtid_seq)
{
  OMS_INFO("Begin to purge binlog to file: {}", to_binlog_index);
  bool found = false;
  for (auto rit = index_records.rbegin(); rit != index_records.rend();) {
    if (strcmp((*rit)->get_file_name().c_str(), to_binlog_index.c_str()) == 0) {
      found = true;
    }

    // The binlog file being written cannot be deleted
    if (found && !is_active((*rit)->get_file_name())) {
      purge_binlog_files.emplace_back((*rit)->get_file_name());
      if (0 == last_gtid_seq) {
        last_gtid_seq = (*rit)->get_current_mapping().second;
      }

      delete *rit;
      rit = std::vector<BinlogIndexRecord*>::reverse_iterator(index_records.erase((++rit).base()));
    } else {
      ++rit;
    }
  }

  if (!found) {
    error_msg = "Failed to find binlog file " + to_binlog_index;
    OMS_ERROR(error_msg);
    return OMS_FAILED;
  }
  return OMS_OK;
}

int BinlogIndexManager::rewrite_index_file(std::string& error_msg, std::vector<BinlogIndexRecord*>& index_records)
{
  std::lock_guard<std::recursive_mutex> op_lock(_op_mutex);
  std::vector<BinlogIndexRecord*>::iterator it;
  std::string temp_index_filename = _index_filename + ".remove";
  std::stringstream records;
  for (it = index_records.begin(); it != index_records.end(); ++it) {
    records << (*it)->to_string();
  }

  int ret = FsUtil::write_file(temp_index_filename, records.str());
  if (OMS_OK != ret) {
    error_msg = "Failed to write file: " + temp_index_filename;
    OMS_ERROR(error_msg);
    return OMS_FAILED;
  }

  std::error_code err;
  fs::rename(temp_index_filename, _index_filename, err);
  if (err) {
    error_msg = "Failed to rename file " + temp_index_filename + " to " + _index_filename +
                ", error: " + logproxy::system_err(errno);
    OMS_ERROR(error_msg);
    return OMS_FAILED;
  }
  return OMS_OK;
}

int BinlogIndexManager::backup_binlog(const BinlogIndexRecord& index_record)
{
  std::string ts = std::to_string(Timer::now_s());
  std::string path = RECOVER_BACKUP_PATH;
  error_code err;
  if (!fs::exists(path, err)) {
    fs::create_directory(path, err);
    if (err) {
      OMS_ERROR("Failed to create directory: {}, error: {}", path, err.message());
      return OMS_FAILED;
    }
  }
  std::string backup_binlog_path = path + s_meta.instance_name() + "_" +
                                   binlog::CommonUtils::fill_binlog_file_name(index_record.get_index()) + "_" + ts;
  OMS_INFO("Begin to backup binlog [{}] to [{}]", index_record.get_file_name(), backup_binlog_path);
  fs::copy(index_record.get_file_name(), backup_binlog_path, fs::copy_options::overwrite_existing, err);
  if (err) {
    OMS_ERROR("Failed to copy binlog from [{}] to [{}], error: {}",
        index_record.get_file_name(),
        backup_binlog_path,
        err.message());
    return OMS_FAILED;
  }

  std::string backup_index = path + s_meta.instance_name() + "_" + "index" + "_" + ts;
  OMS_INFO("Begin to backup index [{}] to [{}]", _index_filename, backup_index);
  fs::copy(_index_filename, backup_index, err);
  if (err) {
    OMS_ERROR("Failed to copy index file from [{}] to [{}], error: {}", _index_filename, backup_index, err.message());
    return OMS_FAILED;
  }

  return OMS_OK;
}

// <------------  BinlogIndexRecord   ------------->
BinlogIndexRecord::BinlogIndexRecord(std::string file_name, int index) : _file_name(std::move(file_name)), _index(index)
{}

const std::pair<std::string, uint64_t>& BinlogIndexRecord::get_before_mapping() const
{
  return _before_mapping;
}

void BinlogIndexRecord::set_before_mapping(const std::pair<std::string, uint64_t>& before_mapping)
{
  BinlogIndexRecord::_before_mapping = before_mapping;
}

const std::pair<std::string, uint64_t>& BinlogIndexRecord::get_current_mapping() const
{
  return _current_mapping;
}

void BinlogIndexRecord::set_current_mapping(const std::pair<std::string, uint64_t>& current_mapping)
{
  BinlogIndexRecord::_current_mapping = current_mapping;
}

const std::string& BinlogIndexRecord::get_file_name() const
{
  return _file_name;
}

void BinlogIndexRecord::set_file_name(const std::string& file_name)
{
  _file_name = file_name;
}

uint64_t BinlogIndexRecord::get_index() const
{
  return _index;
}

void BinlogIndexRecord::set_index(uint64_t index)
{
  _index = index;
}

uint64_t BinlogIndexRecord::get_checkpoint() const
{
  return _checkpoint;
}

void BinlogIndexRecord::set_checkpoint(uint64_t checkpoint)
{
  _checkpoint = checkpoint;
}

uint64_t BinlogIndexRecord::get_position() const
{
  return _position;
}

void BinlogIndexRecord::set_position(uint64_t position)
{
  _position = position;
}

std::string BinlogIndexRecord::to_string() const
{
  std::stringstream str;
  str << _file_name << "\t" << _index << "\t" << get_mapping_str(_current_mapping) << "\t"
      << get_mapping_str(_before_mapping) << "\t" << _checkpoint << "\t" << _position << std::endl;
  return str.str();
}

void BinlogIndexRecord::parse(const std::string& content)
{
  if (content.empty()) {
    OMS_WARN("Failed to parse empty index record");
    return;
  }
  std::vector<std::string> set;
  split(content, '\t', set);
  if (set.size() == 6) {
    this->_file_name = set.at(0);
    this->_index = std::stol(set.at(1));
    this->_current_mapping = parse_mapping_str(set.at(2));
    this->_before_mapping = parse_mapping_str(set.at(3));
    this->_checkpoint = std::stol(set.at(4));
    this->_position = std::stol(set.at(5));
  } else {
    OMS_WARN("Failed to fetch index record, value: {}, set size: {}", content, set.size());
  }
}

std::string BinlogIndexRecord::get_mapping_str(const std::pair<std::string, int64_t>& mapping)
{
  return mapping.first + "=" + std::to_string(mapping.second);
}

std::pair<std::string, int64_t> BinlogIndexRecord::parse_mapping_str(const std::string& mapping_str)
{
  std::pair<std::string, int64_t> mapping_pair;
  if (mapping_str.empty()) {
    return mapping_pair;
  }
  unsigned long pos = mapping_str.find('=');
  mapping_pair.first = mapping_str.substr(0, pos);
  mapping_pair.second = std::stol(mapping_str.substr(pos + 1, mapping_str.size()));
  return mapping_pair;
}

bool BinlogIndexRecord::equal_to(const BinlogIndexRecord& index_record)
{
  return strcmp(_file_name.c_str(), index_record.get_file_name().c_str()) == 0 && _index == index_record.get_index();
}

}  // namespace oceanbase::binlog
