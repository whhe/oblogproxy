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

#include <sstream>
#include "log.h"
#include "counter.h"

namespace oceanbase {
namespace logproxy {
void Counter::stop()
{
  if (is_run()) {
    Thread::stop();
    _sleep_cv.notify_all();
  }
}

void Counter::run()
{
  OMS_INFO("#### Counter thread running, tid: {}", tid());

  std::stringstream ss;
  while (is_run()) {
    _timer.reset();
    this->sleep();
    uint64_t interval_ms = _timer.elapsed() / 1000;
    uint64_t interval_s = interval_ms == 0 ? 0 : (interval_ms / 1000);

    uint64_t rcount = _read_count.load();
    uint64_t wcount = _write_count.load();
    uint64_t ccount = _convert_count.load();
    uint64_t rio = _read_io.load();
    uint64_t wio = _write_io.load();
    uint64_t xwio = _xwrite_io.load();
    uint64_t avg_size = wcount == 0 ? 0 : (wio / wcount);
    uint64_t xavg_size = wcount == 0 ? 0 : (xwio / wcount);
    uint64_t rrps = interval_s == 0 ? rcount : (rcount / interval_s);
    _write_rps = interval_s == 0 ? wcount : (wcount / interval_s);
    _convert_rps = interval_s == 0 ? ccount : (ccount / interval_s);
    uint64_t rios = interval_s == 0 ? rio : (rio / interval_s);
    _write_iops = interval_s == 0 ? wio : (wio / interval_s);
    uint64_t xwios = interval_s == 0 ? xwio : (xwio / interval_s);
    uint64_t delay = Timer::now() - _timestamp_us;
    uint64_t chk_delay = Timer::now() - _checkpoint_us;

    // TODO... bytes rate

    ss.str("");
    ss << "Counter:[Span:" << interval_ms << "ms][Delay:" << delay << "," << chk_delay << "][RCNT:" << rcount
       << "][RRPS:" << rrps << "][RIOS:" << rios << "][WCNT:" << wcount << "][WRPS:" << _write_rps
       << "][WIOS:" << _write_iops << ",AVG:" << avg_size << "][XWIOS:" << xwios << ",AVG:" << xavg_size << "]";
    for (auto& count : _counts) {
      uint64_t c = count.count.load();
      ss << "[" << count.name << ":" << c << "]";
      count.count.fetch_sub(c);
    }
    for (auto& entry : _gauges) {
      ss << "[" << entry.first << ":" << entry.second() << "]";
    }
    OMS_STREAM_INFO << ss.str();

    // sub count that logged
    _read_count.fetch_sub(rcount);
    _write_count.fetch_sub(wcount);
    _convert_count.fetch_sub(ccount);
    _read_io.fetch_sub(rio);
    _write_io.fetch_sub(wio);
    _xwrite_io.fetch_sub(xwio);
  }

  reset();
  OMS_INFO("#### Counter thread stop, tid: {}", tid());
}

void Counter::register_gauge(const std::string& key, const std::function<int64_t()>& func)
{
  _gauges.emplace(key, func);
}

void Counter::count_read(uint64_t count)
{
  _read_count.fetch_add(count);
}

void Counter::count_write(uint64_t count)
{
  _write_count.fetch_add(count);
}

void Counter::count_read_io(uint64_t bytes)
{
  _read_io.fetch_add(bytes);
}

void Counter::count_write_io(uint64_t bytes)
{
  _write_io.fetch_add(bytes);
}

void Counter::count_xwrite_io(uint64_t bytes)
{
  _xwrite_io.fetch_add(bytes);
}

void Counter::count_key(Counter::CountKey key, uint64_t count)
{
  _counts[key].count.fetch_add(count);
}

void Counter::mark_timestamp(uint64_t timestamp_us)
{
  _count_timestamp_us = Timer::now();
  _timestamp_us = timestamp_us;
}

uint64_t Counter::delay_us() const
{
  return Timer::now() - _timestamp_us;
}

void Counter::mark_checkpoint(uint64_t checkpoint_us)
{
  _checkpoint_us = checkpoint_us;
}

void Counter::count_convert(uint64_t count)
{
  _convert_count.fetch_add(count);
}

uint64_t Counter::convert_rps() const
{
  return _convert_rps;
}

uint64_t Counter::write_rps() const
{
  return _write_rps;
}

uint64_t Counter::write_iops() const
{
  return _write_iops;
}

uint64_t Counter::checkpoint_us() const
{
  return _checkpoint_us;
}

void Counter::sleep()
{
  // condition variable as SLEEP which could be gracefully interrupted
  std::unique_lock<std::mutex> lk(_sleep_cv_lk);
  _sleep_cv.wait_for(lk, std::chrono::seconds(_sleep_interval_s));
}

void Counter::reset()
{
  _read_count = 0;
  _read_io = 0;
  _write_count = 0;
  _write_io = 0;
  _xwrite_io = 0;
  _convert_count = 0;
  // delay
  _count_timestamp_us = 0;
  _checkpoint_us = 0;
  _timestamp_us = 0;
  // rps
  _convert_rps = 0;
  _write_rps = 0;
  _write_iops = 0;
}

void CounterStatistics::stop()
{
  Thread::stop();
  _sleep_cv.notify_all();
  join();
}

void CounterStatistics::run()
{
  OMS_INFO("#### Dumper counter thread running, tid: {}", tid());

  std::stringstream ss;
  while (is_run()) {
    _timer.reset();
    this->sleep();
    int64_t interval_ms = _timer.elapsed() / 1000;

    ss.str("");
    ss << "Counter:[Span:" << interval_ms << "ms]";

    for (auto& entry : _gauges) {
      if (entry.second != nullptr) {
        ss << "[" << entry.first << ":" << entry.second() << "]";
      }
    }
    for (const auto& s_entry : _str_guages) {
      if (s_entry.second != nullptr) {
        ss << "[" << s_entry.first << ":" << s_entry.second() << "]";
      }
    }
    OMS_INFO(ss.str());
  }

  OMS_INFO("#### Dumper counter thread stop, tid: {}", tid());
}

void CounterStatistics::register_gauge(const std::string& key, const std::function<uint64_t()>& func)
{
  _gauges.emplace(key, func);
}

void CounterStatistics::sleep()
{
  // condition variable as SLEEP which could be gracefully interrupted
  std::unique_lock<std::mutex> lk(_sleep_cv_lk);
  _sleep_cv.wait_for(lk, std::chrono::seconds(_sleep_interval_s));
}

void CounterStatistics::unregister_gauge(std::string const& key)
{
  if (_gauges.find(key) != _gauges.end()) {
    _gauges.erase(key);
  } else if (_str_guages.find(key) != _str_guages.end()) {
    _str_guages.erase(key);
  } else {
    OMS_ERROR("Not found key in dumper guages: {}", key);
  }
}

void CounterStatistics::register_gauge(const std::string& key, const std::function<std::string()>& func)
{
  _str_guages.emplace(key, func);
}

}  // namespace logproxy
}  // namespace oceanbase
