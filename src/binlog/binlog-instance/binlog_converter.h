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

#include "common.h"
#include "obcdc_config.h"
#include "obcdcaccess/obcdc_factory.h"
#include "obaccess/ob_access.h"
#include "binlog_storage.h"
#include "clog_reader_routine.h"
#include "instance_socket_listener.h"
#include "parallel_convert.h"

#include <env.h>

namespace oceanbase::binlog {

class BinlogConverter : public Thread {
public:
  static BinlogConverter& instance()
  {
    static BinlogConverter converter_singleton;
    return converter_singleton;
  }

public:
  explicit BinlogConverter();

  ~BinlogConverter() override;

  int init();

  void run() override;

  void cancel();

  void stop_converter();

  void join_converter();

public:
  void update_throttle_rps(uint32_t rps);

  void update_throttle_iops(uint64_t iops);

private:
  int start_internal();

private:
  IObCdcAccess* _obcdc = nullptr;
  BlockingQueue<ILogRecord*> _queue{s_meta.binlog_config()->record_queue_size()};
  BlockingQueue<ObLogEvent*> _event_queue{s_meta.binlog_config()->record_queue_size()};
  ClogReaderRoutine _reader{*this, _queue};
  ParallelConvert _convert{*this, _queue, _event_queue};
  BinlogStorage _storage{*this, _event_queue};
};
}  // namespace oceanbase::binlog
