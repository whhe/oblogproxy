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

#include <mutex>

#include "timer.h"

using namespace oceanbase::logproxy;
namespace oceanbase::binlog {
class TokenBucket {
public:
  static TokenBucket* create(double permits_per_second);

  TokenBucket() = default;
  virtual ~TokenBucket() = default;

public:
  double acquire();

  double acquire(uint32_t permits);

  void set_rate(double permits_per_second);

  double get_rate();

private:
  uint64_t reserve(uint32_t permits);

  uint64_t reserve_and_get_wait_length(uint32_t permits, uint64_t now_micros);

protected:
  virtual void do_set_rate(double, uint64_t now_micros) = 0;

  virtual double do_get_rate() = 0;

  virtual uint64_t reserve_earliest_available(uint32_t, uint64_t) = 0;

private:
  std::mutex _mutex;
};

class SmoothTokenBucket : public TokenBucket {
public:
  ~SmoothTokenBucket() override = default;

  explicit SmoothTokenBucket(double max_burst_seconds);

protected:
  void do_set_rate(double permits_per_sec, uint64_t now_micros) override;

  double do_get_rate() override;

  uint64_t reserve_earliest_available(uint32_t, uint64_t) override;

private:
  void resync(uint64_t now_micros);

  void do_set_rate(double permits_per_sec);

private:
  const double _max_burst_seconds;

  double _stored_permits = 0.0;
  double _max_permits = 0.0;

  double _stable_interval_micros = 0.0;
  uint64_t _next_free_ticket_micros = 0L;
};

}  // namespace oceanbase::binlog