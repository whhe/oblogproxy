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

#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <mutex>
#include "ob_log_event.h"
#include "timer.h"

#define GTID_SEQ_FILENAME "binlog_tenant_gtid_seq.meta"

namespace oceanbase::binlog {

static std::function<std::string(GtidMessage*)> gtid_str_generator = [](GtidMessage* gtid) {
  return gtid->format_string();
};

class GtidSeq {
public:
  GtidSeq() = default;

  explicit GtidSeq(uint64_t commit_version_start, std::string xid_start, uint64_t gtid_start, uint64_t trxs_num);

  std::string serialize() const;

  void deserialize(std::string line);

  uint64_t get_gtid_start() const
  {
    return _gtid_start;
  }

  uint64_t get_commit_version_start() const
  {
    return _commit_version_start;
  }

  std::string get_xid_start()
  {
    return _xid_start;
  }

  uint64_t get_trxs_num() const
  {
    return _trxs_num;
  }

private:
  uint64_t _commit_version_start = 0;
  std::string _xid_start;
  uint64_t _gtid_start = 0;
  uint64_t _trxs_num = 0;
};

class GtidManager {
public:
  explicit GtidManager(std::string gtid_seq_filename);

  ~GtidManager();

public:
  int init();

  void init_start_timestamp(uint64_t start_timestamp);

  void mark_event(bool is_heartbeat, uint64_t checkpoint, const std::pair<std::string, uint64_t>& trx_mapping);

  void compress_and_save_gtid_seq(const std::pair<std::string, uint64_t>& trx_mapping);

  void purge_gtid_before(uint64_t gtid_seq);

  void get_instance_incr_gtids_string(uint64_t timestamp, std::string& gtids);

  int get_latest_gtid_seq(GtidSeq& gtid_seq);

  void get_last_gtid_purged(uint64_t& last_gtid_purged) const;

  void get_last_checkpoint_purged(uint64_t& last_checkpoint_purged) const;

  int get_first_le_gtid_seq(uint64_t gtid, GtidSeq& gtid_seq);

  void update_gtid_executed(uint64_t gtid);

  void init_gtid_seq();

private:
  int init_gtid_variables();

  int save_gtid_seq(const GtidSeq& gtid_seq);

  void update_gtid_purged(uint64_t gtid);

  void clear_outdated_heartbeat_gtid_seqs_periodically(uint64_t current_checkpoint);

  void read_last_valid_gtid_seq(GtidSeq& gtid_seq);

private:
  void rewrite_gtid_file(const std::function<bool(GtidSeq&)>& compare_func, bool update_first_checkpoint = false);

private:
  std::timed_mutex _op_mutex;

  uint64_t _nof_trx = 0;
  uint64_t _curr_hb_checkpoint = 0;
  uint64_t _curr_trx_checkpoint = 0;
  uint64_t _last_saved_commit_version_high = 0;
  std::string _gtid_seq_filename;
  bool first_entry_after_restarted = false;
  std::vector<GtidSeq> _gtid_checkpoint_set;

  uint64_t _last_gtid_purged = 0;
  uint64_t _last_checkpoint_purged = 0;
  std::vector<std::pair<uint64_t, std::string>> _gtid_seq_set;
  std::vector<GtidMessage*> _gtid_purged_set;
  std::vector<GtidMessage*> _gtid_executed_set;
};

int binary_search_gtid_seq(uint64_t timestamp, const std::string& tenant_gtids_str, GtidSeq& gtid_seq);

int boundary_gtid_seq(
    const std::string& tenant_gtids_str, std::pair<GtidSeq, GtidSeq>& gtid_seq_pair, std::string instance = "");

void merge_gtid_before(std::vector<GtidMessage*>& gtid_set, txn_range gtid_range);

}  // namespace oceanbase::binlog
