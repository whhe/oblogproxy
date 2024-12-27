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

#include "binlog_converter.h"

#include "env.h"
#include "counter.h"

namespace oceanbase::binlog {

BinlogConverter::BinlogConverter() : Thread("BinlogConverter")
{}

BinlogConverter::~BinlogConverter()
{
  //  stop_converter();
  //  join_converter();
  ObCdcAccessFactory::unload(_obcdc);
}

int BinlogConverter::init()
{
  Counter::instance().register_gauge("RecordQueueSize", [this]() { return _queue.size(); });

  // load different so library according to ob version
  int ret = ObCdcAccessFactory::load(s_meta.get_obcdc_config(), _obcdc);
  if (OMS_OK != ret) {
    return ret;
  }

  /*
   * If restarted, start_timestamp will be set when init convert
   */
  ret = _convert.init(_obcdc);
  if (ret != OMS_OK) {
    return ret;
  }
  OMS_INFO("Convert initialized successfully");

  ret = _storage.init();
  if (ret != OMS_OK) {
    return ret;
  }
  OMS_INFO("Storage initialized successfully");

  return _reader.init(s_meta.get_obcdc_config(), _obcdc);
}

void BinlogConverter::run()
{
  int ret = init();
  if (OMS_OK == ret) {
    OMS_INFO("Successfully init binlog converter, and begin to start binlog converter.");

    start_internal();
  } else {
    OMS_ERROR("Failed to init binlog converter.");
    ::exit(-1);
  }
}

int BinlogConverter::start_internal()
{
  Counter::instance().start();
  _storage.start();
  _convert.start();
  _reader.start();
  return OMS_OK;
}

void BinlogConverter::join_converter()
{
  _reader.join();
  _convert.join();
  _storage.join();
}

void BinlogConverter::stop_converter()
{
  if (is_run()) {
    _reader.stop();
    _convert.stop();
    _storage.stop();
    Counter::instance().stop();
    Thread::stop();
  }
  OMS_ERROR("Binlog converter has been stopped.");
  // The entire program needs to exit
  ::exit(-1);
}

void BinlogConverter::cancel()
{
  _reader.set_run(false);
  _convert.set_run(false);
  _storage.set_run(false);
}

void BinlogConverter::update_throttle_rps(uint32_t rps)
{
  _storage.get_rate_limiter().update_throttle_rps(rps);
}

void BinlogConverter::update_throttle_iops(uint64_t iops)
{
  _storage.get_rate_limiter().update_throttle_iops(iops);
}

}  // namespace oceanbase::binlog
