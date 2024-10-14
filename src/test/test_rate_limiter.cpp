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

#include "gtest/gtest.h"

#include "rate_limiter.h"

using namespace oceanbase::binlog;
TEST(EventRateLimiter, rate_limiter)
{
  // case 1: rps
  {
    EventRateLimiter limiter;
    limiter.update_throttle_rps(100);
    Timer timer;
    for (int i = 0; i < 200; i++) {
      limiter.in_event_with_alarm(100);
    }
    ASSERT_GE(timer.elapsed(), 1900000);

    timer.reset();
    limiter.update_throttle_rps(0);
    for (int i = 0; i < 200; i++) {
      limiter.in_event_with_alarm(100);
    }
    ASSERT_LT(timer.elapsed(), 500000);
  }

  // case 2: iops
  {
    EventRateLimiter limiter;
    limiter.update_throttle_iops(20000000);
    Timer timer;
    for (int i = 0; i < 200; i++) {
      limiter.in_event_with_alarm(200000);
    }
    ASSERT_GE(timer.elapsed(), 1900000);

    timer.reset();
    limiter.update_throttle_iops(0);
    for (int i = 0; i < 200; i++) {
      limiter.in_event_with_alarm(200000);
    }
    ASSERT_LT(timer.elapsed(), 500000);
  }

  // case 3: rps + iops
  {
    EventRateLimiter limiter;
    limiter.update_throttle_rps(100);
    limiter.update_throttle_iops(20000000);
    Timer timer;
    for (int i = 0; i < 200; i++) {
      limiter.in_event_with_alarm(200000);
    }
    ASSERT_GE(timer.elapsed(), 1900000);

    timer.reset();
    limiter.update_throttle_rps(50);
    for (int i = 0; i < 200; i++) {
      limiter.in_event_with_alarm(200000);
    }
    ASSERT_GE(timer.elapsed(), 3900000);
  }
  {
    EventRateLimiter limiter;
    Timer timer;
    limiter.update_throttle_rps(50);
    limiter.update_throttle_iops(5000000);
    for (int i = 0; i < 200; i++) {
      limiter.in_event_with_alarm(200000);
    }
    ASSERT_GE(timer.elapsed(), 7900000);
  }
}