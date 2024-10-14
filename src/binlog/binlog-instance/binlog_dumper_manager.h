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

#include <map>
#include <shared_mutex>

#include "binlog_dumper.h"
#include "binlog/rate_limiter.h"

namespace oceanbase::binlog {

class BinlogDumperManager : public Limiter {
public:
  BinlogDumperManager();
  explicit BinlogDumperManager(uint16_t total);
  explicit BinlogDumperManager(uint16_t total, double cpu_limit);

  ~BinlogDumperManager() = default;

  BinlogDumperManager(const BinlogDumperManager&) = delete;
  BinlogDumperManager(const BinlogDumperManager&&) = delete;
  BinlogDumperManager& operator=(const BinlogDumperManager&) = delete;
  BinlogDumperManager& operator=(const BinlogDumperManager&&) = delete;

public:
  void add(BinlogDumper* dumper);

  void remove(BinlogDumper* dumper);

  void get_dump_info(std::vector<DumpInfo>& dump_info_vec);

  uint32_t dumper_num();

  bool can_add_one();

  uint64_t min_dumper_checkpoint();

public:
  void update_total_limit(uint16_t total);

  void update_throttle_rps(uint32_t rps);

  void update_throttle_iops(uint64_t iops);

  void mark_dump_error_count()
  {
    std::unique_lock<std::shared_mutex> exclusive_lock{_shared_mutex};
    OMS_ATOMIC_INC(_error_count);
  }

  uint64_t get_dump_error_count()
  {
    std::unique_lock<std::shared_mutex> exclusive_lock{_shared_mutex};
    return _error_count;
  }

private:
  std::shared_mutex _shared_mutex;
  std::map<std::string, BinlogDumper*> _dumpers;

  uint16_t _total_limit = 0;
  double _cpu_limit = 0.0;

  std::uint64_t _error_count = 0;
};

}  // namespace oceanbase::binlog
