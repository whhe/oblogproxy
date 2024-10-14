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

#ifndef OMS_LOGPROXY_BINLOG_CONVERT_H
#define OMS_LOGPROXY_BINLOG_CONVERT_H

#include "thread.h"
#include "timer.h"
#include "blocking_queue.hpp"
#include "str_array.h"
#include "obcdc_config.h"
#include "codec/message.h"
#include "obcdcaccess/obcdc_factory.h"

#include "binlog/ob_log_event.h"
#include "binlog/binlog_index.h"
#include "binlog/ddl-parser/ddl_parser.h"
#include "table_cache.h"
using namespace oceanbase::logproxy;

namespace oceanbase::binlog {
class BinlogConverter;

class BinlogConvert : public Thread {
public:
  BinlogConvert(
      BinlogConverter& converter, BlockingQueue<ILogRecord*>& rqueue, BlockingQueue<ObLogEvent*>& event_queue);

  int init(IObCdcAccess* obcdc);

  void stop() override;

  void run() override;

  void append_event(BlockingQueue<ObLogEvent*>& queue, ObLogEvent* event);

  bool convert_gtid_log_event(ILogRecord* record, bool is_ddl = false);

  void convert_query_event(ILogRecord* record);

  bool ddl_need_to_be_stored(ILogRecord* record);

  void convert_xid_event(ILogRecord* record);

  void convert_table_map_event(ILogRecord* record);

  void convert_write_rows_event(ILogRecord* record);

  void convert_delete_rows_event(ILogRecord* record);

  void convert_update_rows_event(ILogRecord* record);

  void do_convert(const std::vector<ILogRecord*>& records);

  void get_before_images(ILogRecord* record, int col_count, MsgBuf& col_data) const;

  void get_after_images(ILogRecord* record, int col_count, MsgBuf& col_data) const;

private:
  int init_restart_point(const BinlogIndexRecord& index_record, GtidLogEvent* event);

  int recover_safely();

  void pass_heartbeat_checkpoint(ILogRecord* record);

  void rotate_binlog_file(uint64_t timestamp);

  void merge_previous_gtids(uint64_t last_gtid, std::vector<GtidMessage*>& previous_gtids);

  void set_start_pos(const std::vector<GtidMessage*>& previous_gtids);

  uint64_t table_id(const string& db_name, const string& tb_name);

  void refresh_table_cache(const string& db_name, const string& tb_name);

private:
  BinlogConverter& _converter;
  IObCdcAccess* _obcdc;
  BlockingQueue<ILogRecord*>& _rqueue;
  BlockingQueue<ObLogEvent*>& _event_queue;
  Timer _stage_timer;
  uint64_t _txn_gtid_seq = 1;
  uint64_t _xid = 0;
  uint32_t _cur_pos = 0;
  uint64_t _binlog_file_index = 0;
  std::pair<std::string /* ob_txn_id */, uint64_t /* ob_txn_gtid_seq */> _txn_mapping;
  bool _filter_util_checkpoint_trx = true;
  bool _meet_initial_trx = false;
  bool _filter_checkpoint_trx = true;
  uint64_t _start_timestamp = 0;
#ifndef BUILD_OPENSOURCE
  binlog::DdlParser _ddl_parser;
#endif
  uint64_t _first_gtid_seq = 1;
  std::vector<GtidMessage*> _previous_gtid_messages;
  TableCache _table_cache;
};

void fill_bitmap(int col_count, int col_bytes, unsigned char* bitmap);
}  // namespace oceanbase::binlog

#endif  // OMS_LOGPROXY_BINLOG_CONVERT_H
