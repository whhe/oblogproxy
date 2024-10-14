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

#ifndef OMS_LOGPROXY_BINLOG_STORAGE_H
#define OMS_LOGPROXY_BINLOG_STORAGE_H

#include <cassert>

#include "thread.h"
#include "timer.h"
#include "log.h"
#include "ob_log_event.h"
#include "blocking_queue.hpp"
#include "obcdcaccess/obcdc/obcdc_entry.h"
#include "binlog_index.h"
#include "data_type.h"
#include "obcdc_config.h"
#include "gtid_manager.h"
#include "rate_limiter.h"

namespace oceanbase::binlog {
class BinlogConverter;

class BinlogStorage : public Thread {
public:
  int init();

  void stop() override;

  void run() override;

  BinlogStorage(BinlogConverter& reader, BlockingQueue<ObLogEvent*>& event_queue);

  size_t rotate(ObLogEvent* event, MsgBuf& content, std::size_t size, BinlogIndexRecord& index_record);

  int storage_binlog_event(
      vector<ObLogEvent*>& records, MsgBuf& buffer, size_t& buffer_pos, BinlogIndexRecord& index_record);

  int finishing_touches(MsgBuf& buffer, size_t& buffer_pos);
  /*
   The rotation action is for the previous binlog file
   */
  int rotate_current_binlog(MsgBuf& content, size_t size, RotateEvent* rotate_event, BinlogIndexRecord& index_record);

  /*
   *The rotation action is for the current binlog file
   */
  int add_next_binlog_index(BinlogIndexRecord& index_record, const RotateEvent* rotate_event);

public:
  EventRateLimiter& get_rate_limiter()
  {
    return _rate_limiter;
  }

private:
  void update_index_record(BinlogIndexRecord& index_record);

  static void purge_expired_binlog(BinlogIndexRecord& index_record);

private:
  BlockingQueue<ObLogEvent*>& _event_queue;
  std::string _file_name;
  Timer _stage_timer;
  BinlogConverter& _converter;
  uint64_t _offset;
  bool _valid_events_coming = false;

  EventRateLimiter _rate_limiter;
};
}  // namespace oceanbase::binlog

#endif  // OMS_LOGPROXY_BINLOG_STORAGE_H
