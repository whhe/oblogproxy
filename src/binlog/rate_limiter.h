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

#include "token_bucket.h"

using namespace oceanbase::logproxy;
namespace oceanbase::binlog {
class Limiter {};

class EventRateLimiter : public Limiter {
public:
  EventRateLimiter() = default;

  EventRateLimiter(uint32_t throttle_rps, uint64_t throttle_iops);

  ~EventRateLimiter();

public:
  void update_throttle_rps(uint32_t throttle_rps);

  void update_throttle_iops(uint64_t throttle_iops);

public:
  double in_event(uint32_t event_len);  // todo uint32 or 64?

  void in_event_with_alarm(uint32_t event_len);

private:
  std::mutex _mutex;
  TokenBucket* _rps_token_bucket = nullptr;
  TokenBucket* _iops_token_bucket = nullptr;
};

}  // namespace oceanbase::binlog
