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

#include "clog_reader_routine.h"

#include "counter.h"
#include "trace_log.h"
#include "binlog_converter.h"

#include <env.h>

namespace oceanbase::binlog {

static Config& _s_config = Config::instance();

ClogReaderRoutine::ClogReaderRoutine(BinlogConverter& converter, BlockingQueue<ILogRecord*>& queue)
    : Thread("Clog Reader Routine"), _converter(converter), _obcdc(nullptr), _queue(queue)
{}

int ClogReaderRoutine::init(const ObcdcConfig& config, IObCdcAccess* obcdc)
{
  _obcdc = obcdc;
  std::map<std::string, std::string> configs;
  config.generate_configs(configs);
  if (config.start_timestamp_us.val() != 0) {
    OMS_INFO("Init clog reader with start_timestamp_us: {}", config.start_timestamp_us.val());
    return _obcdc->init_with_us(configs, config.start_timestamp_us.val());
  } else {
    OMS_INFO("Init clog reader with start_timestamp: {}", config.start_timestamp.val());
    return _obcdc->init(configs, config.start_timestamp.val());
  }
}

void ClogReaderRoutine::stop()
{
  if (is_run()) {
    Thread::stop();
    // obcdc stop interface will have various core problems. CDC classmates suggested that you directly use the process
    // exit process to force termination.
    //  _obcdc->stop();
    _queue.clear([this](ILogRecord* record) { _obcdc->release(record); });
  }
  OMS_INFO("Begin to stop clog reader routine thread...");
}

void ClogReaderRoutine::run()
{
  if (_obcdc->start() != OMS_OK) {
    OMS_ERROR("Failed to start clog reader routine.");
    _converter.stop_converter();
    return;
  }

  Counter& counter = Counter::instance();
  Timer stage_tm;
  while (is_run()) {
    stage_tm.reset();
    ILogRecord* record = nullptr;
    int ret = _obcdc->fetch(record, _s_config.read_timeout_us.val());
    int64_t fetch_us = stage_tm.elapsed();

    if (ret == OB_TIMEOUT && record == nullptr) {
      OMS_DEBUG("Fetch libobcdc timeout, nothing coming...");
      continue;
    }

    if (ret != OB_SUCCESS) {
      OMS_ERROR("Failed to get data from liboblog, error code: {}", ret);
      break;
    }

    if (_s_config.verbose_record_read.val()) {
      TraceLog::info(record);
    }

    stage_tm.reset();
    counter.count_read_io(record->getRealSize());
    counter.count_read(1);
    while (!_queue.offer(record, s_meta.binlog_config()->read_timeout_us())) {
      OMS_DEBUG("Reader transfer queue full({}), retry...", _queue.size(false));
    }
    int64_t offer_us = stage_tm.elapsed();

    counter.count_key(Counter::READER_FETCH_US, fetch_us);
    counter.count_key(Counter::READER_OFFER_US, offer_us);
  }
  _converter.stop_converter();
}

}  // namespace oceanbase::binlog