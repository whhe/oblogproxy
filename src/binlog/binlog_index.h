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

#pragma once

#include <string>
#include <vector>
#include <filesystem>

#include "fs_util.h"

#define BINLOG_INDEX_NAME "./data/mysql-bin.index"
#define RECOVER_BACKUP_PATH "../recover_backup/"

namespace oceanbase::binlog {
class BinlogIndexRecord {
public:
  BinlogIndexRecord(std::string file_name, int index);

  BinlogIndexRecord() = default;

  const std::pair<std::string, uint64_t>& get_before_mapping() const;

  void set_before_mapping(const std::pair<std::string, uint64_t>& before_mapping);

  const std::pair<std::string, uint64_t>& get_current_mapping() const;

  void set_current_mapping(const std::pair<std::string, uint64_t>& current_mapping);

  const std::string& get_file_name() const;

  void set_file_name(const std::string& file_name);

  uint64_t get_index() const;

  void set_index(uint64_t index);

  uint64_t get_checkpoint() const;

  void set_checkpoint(uint64_t checkpoint);

  uint64_t get_position() const;

  void set_position(uint64_t position);

  std::string to_string() const;

  void parse(const std::string& content);

  bool equal_to(const BinlogIndexRecord& index_record);

private:
  static std::string get_mapping_str(const std::pair<std::string, int64_t>& mapping);

  static std::pair<std::string, int64_t> parse_mapping_str(const std::string& mapping_str);

private:
  std::string _file_name;
  uint64_t _index = 0;
  // mapping of ob txn and mysql txn
  std::pair<std::string, uint64_t> _before_mapping;
  std::pair<std::string, uint64_t> _current_mapping;
  uint64_t _checkpoint = 0;
  uint64_t _position = 0;
};

class BinlogIndexManager {
public:
  explicit BinlogIndexManager(std::string index_filename);

  ~BinlogIndexManager() = default;

  int fetch_index_vector(std::vector<BinlogIndexRecord*>& index_records);

  int add_index(const BinlogIndexRecord& record);

  int get_index(const std::string& binlog_file, BinlogIndexRecord& record);

  int get_first_index(BinlogIndexRecord& record);

  int get_latest_index(BinlogIndexRecord& record, size_t index = 1);

  int purge_binlog_index(const std::string& base_name, const std::string& binlog_file,
      const std::string& before_purge_ts, std::string& error_msg, std::vector<std::string>& purge_binlog_files);

  int update_index(const BinlogIndexRecord& record, size_t pos = 2);

  int remove_binlog(const BinlogIndexRecord& index_record);

  bool is_active(const std::string& file);

  void get_memory_index(BinlogIndexRecord& record);

  bool is_behind_current_pos(const std::string& file, uint64_t pos, uint64_t wait_time_us);

  int rewrite_index_file(std::string& error_msg, std::vector<BinlogIndexRecord*>& index_records);

  int backup_binlog(const BinlogIndexRecord& index_record);

private:
  int purge_binlog_before_ts(const std::string& before_purge_ts, std::vector<BinlogIndexRecord*>& index_records,
      std::string& error_msg, std::vector<std::string>& purge_binlog_files, uint64_t& last_gtid_seq);

  int purge_binlog_to_file(const std::string& to_binlog_index, std::string& error_msg,
      std::vector<BinlogIndexRecord*>& index_records, std::vector<std::string>& purge_binlog_files,
      uint64_t& last_gtid_seq);

private:
  // support for reentrant lock
  std::recursive_mutex _op_mutex;

  // notify_all when add_index and update index
  std::condition_variable_any _new_events;

  std::string _index_filename;

  // update when call update_index()
  BinlogIndexRecord _memory_index_record;
};
}  // namespace oceanbase::binlog
