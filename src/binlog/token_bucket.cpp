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

#include "token_bucket.h"

#include <limits>
#include <thread>

#include "log.h"

namespace oceanbase::binlog {

TokenBucket* TokenBucket::create(double permits_per_sec)
{
  if (permits_per_sec < 0.0) {
    return nullptr;
  }

  auto* token_bucket = new SmoothTokenBucket(1.0);
  token_bucket->set_rate(permits_per_sec);
  return token_bucket;
}

void TokenBucket::set_rate(double permits_per_sec)
{
  std::unique_lock<std::mutex> unique_lock(this->_mutex);
  do_set_rate(permits_per_sec, Timer::now_micros());
}

double TokenBucket::acquire()
{
  return this->acquire(1);
}

double TokenBucket::acquire(uint32_t permits)
{
  if (permits <= 0) {
    OMS_ERROR("Requested permits {} must be positive", permits);
    return 0;
  }

  uint64_t micros_to_wait = this->reserve(permits);
  std::this_thread::sleep_for(std::chrono::microseconds(micros_to_wait));
  return 1.0 * (double)micros_to_wait / 1000000;
}

uint64_t TokenBucket::reserve(uint32_t permits)
{
  std::unique_lock<std::mutex> unique_lock(this->_mutex);
  return reserve_and_get_wait_length(permits, Timer::now_micros());
}

uint64_t TokenBucket::reserve_and_get_wait_length(uint32_t permits, uint64_t now_micros)
{
  uint64_t moment_available = this->reserve_earliest_available(permits, now_micros);
  return std::max(moment_available - now_micros, (uint64_t)0);
}

double TokenBucket::get_rate()
{
  std::unique_lock<std::mutex> unique_lock(this->_mutex);
  return do_get_rate();
}

SmoothTokenBucket::SmoothTokenBucket(double max_burst_seconds) : _max_burst_seconds(max_burst_seconds)
{}

void SmoothTokenBucket::do_set_rate(double permits_per_sec, uint64_t now_micros)
{
  this->resync(now_micros);
  double stable_interval_micros = (1000000 / permits_per_sec);
  this->_stable_interval_micros = stable_interval_micros;
  do_set_rate(permits_per_sec);
}

void SmoothTokenBucket::resync(uint64_t now_micros)
{
  if (now_micros > _next_free_ticket_micros) {
    double new_permits = (double)(now_micros - this->_next_free_ticket_micros) / this->_stable_interval_micros;
    this->_stored_permits = std::min(this->_max_permits, this->_stored_permits + new_permits);
    this->_next_free_ticket_micros = now_micros;
  }
}

void SmoothTokenBucket::do_set_rate(double permits_per_sec)
{
  double old_max_permits = this->_max_permits;
  this->_max_permits = this->_max_burst_seconds * permits_per_sec;
  if (old_max_permits == std::numeric_limits<double>::infinity()) {
    this->_stored_permits = this->_max_permits;
  } else {
    this->_stored_permits =
        (old_max_permits == 0.0) ? 0.0 : this->_stored_permits * this->_max_permits / old_max_permits;
  }
}

uint64_t SmoothTokenBucket::reserve_earliest_available(uint32_t required_permits, uint64_t now_micros)
{
  this->resync(now_micros);
  uint64_t return_value = this->_next_free_ticket_micros;
  double stored_permits_to_spend = std::min((double)required_permits, this->_stored_permits);
  double fresh_permits = required_permits - stored_permits_to_spend;
  auto wait_micros = (uint64_t)(fresh_permits * this->_stable_interval_micros);
  this->_next_free_ticket_micros = this->_next_free_ticket_micros + wait_micros;
  this->_stored_permits -= stored_permits_to_spend;
  return return_value;
}

double SmoothTokenBucket::do_get_rate()
{
  return 1000000 / this->_stable_interval_micros;
}

}  // namespace oceanbase::binlog
