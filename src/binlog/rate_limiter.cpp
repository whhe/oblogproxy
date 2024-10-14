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

#include "rate_limiter.h"

#include "log.h"

namespace oceanbase::binlog {

EventRateLimiter::EventRateLimiter(uint32_t throttle_rps, uint64_t throttle_iops)
{
  update_throttle_rps(throttle_rps);
  update_throttle_iops(throttle_iops);
}

EventRateLimiter::~EventRateLimiter()
{
  if (nullptr != _rps_token_bucket) {
    delete _rps_token_bucket;
    _rps_token_bucket = nullptr;
  }

  if (nullptr != _iops_token_bucket) {
    delete _iops_token_bucket;
    _iops_token_bucket = nullptr;
  }
}

void EventRateLimiter::update_throttle_rps(uint32_t throttle_rps)
{
  std::unique_lock<std::mutex> unique_lock(this->_mutex);
  if (throttle_rps == 0) {
    OMS_INFO("Begin to disable throttle rps: {}", throttle_rps);
    if (nullptr != _rps_token_bucket) {
      delete _rps_token_bucket;
      _rps_token_bucket = nullptr;
    }
  } else {
    if (nullptr == _rps_token_bucket) {
      OMS_INFO("Begin to generate token bucket to limit rps: {}", throttle_rps);
      _rps_token_bucket = TokenBucket::create((double)throttle_rps);
    } else {
      OMS_INFO("Begin to update throttle rps from {} to {}", _rps_token_bucket->get_rate(), throttle_rps);
      _rps_token_bucket->set_rate(throttle_rps);
    }
  }
}

void EventRateLimiter::update_throttle_iops(uint64_t throttle_iops)
{
  std::unique_lock<std::mutex> unique_lock(this->_mutex);
  if (throttle_iops == 0) {
    OMS_INFO("Begin to disable throttle iops: {}", throttle_iops);
    if (nullptr != _iops_token_bucket) {
      delete _iops_token_bucket;
      _iops_token_bucket = nullptr;
    }
  } else {
    if (nullptr == _iops_token_bucket) {
      OMS_INFO("Begin to generate token bucket to limit iops: {}", throttle_iops);
      _iops_token_bucket = TokenBucket::create((double)throttle_iops);
    } else {
      OMS_INFO("Begin to update throttle iops from {} to {}", _iops_token_bucket->get_rate(), throttle_iops);
      _iops_token_bucket->set_rate((double)throttle_iops);
    }
  }
}

double EventRateLimiter::in_event(uint32_t event_len)
{
  std::unique_lock<std::mutex> unique_lock(this->_mutex);
  double sec_to_wait = 0.0;
  if (nullptr != _rps_token_bucket) {
    sec_to_wait += _rps_token_bucket->acquire(1);
  }

  if (nullptr != _iops_token_bucket) {
    sec_to_wait += _iops_token_bucket->acquire(event_len);
  }
  return sec_to_wait;
}

void EventRateLimiter::in_event_with_alarm(uint32_t event_len)
{
  double sec_waited = this->in_event(event_len);
  if (sec_waited > 0.1) {
    OMS_WARN("The waiting time(s) [{}] for limiting rate is too long", sec_waited);
  }
}

}  // namespace oceanbase::binlog
