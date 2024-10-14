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

#include "binlog_dumper_manager.h"

namespace oceanbase::binlog {

BinlogDumperManager::BinlogDumperManager() : BinlogDumperManager(0.0, 0.0)
{}

BinlogDumperManager::BinlogDumperManager(uint16_t total) : BinlogDumperManager(total, 0.0)
{}

BinlogDumperManager::BinlogDumperManager(uint16_t total, double cpu_limit) : _total_limit(total), _cpu_limit(cpu_limit)
{
  Counter::instance().register_gauge("NDUMPER", [this]() { return _dumpers.size(); });
}

void BinlogDumperManager::add(BinlogDumper* dumper)
{
  std::unique_lock<std::shared_mutex> exclusive_lock{_shared_mutex};
  std::string dumper_id = dumper->get_connection()->trace_id();
  while (_dumpers.find(dumper_id) == _dumpers.end()) {
    _dumpers[dumper_id] = dumper;
  }
}
void BinlogDumperManager::remove(BinlogDumper* dumper)
{
  std::unique_lock<std::shared_mutex> exclusive_lock{_shared_mutex};
  _dumpers.erase(dumper->get_connection()->trace_id());
}

void BinlogDumperManager::get_dump_info(std::vector<DumpInfo>& dump_info_vec)
{
  std::unique_lock<std::shared_mutex> exclusive_lock{_shared_mutex};
  for (auto dumper_pair : _dumpers) {
    dump_info_vec.emplace_back(dumper_pair.second->get_dump_info());
  }
}

uint32_t BinlogDumperManager::dumper_num()
{
  std::unique_lock<std::shared_mutex> exclusive_lock{_shared_mutex};
  return _dumpers.size();
}

uint64_t BinlogDumperManager::min_dumper_checkpoint()
{
  std::unique_lock<std::shared_mutex> exclusive_lock{_shared_mutex};
  if (_dumpers.empty()) {
    return 0;
  }

  uint64_t min_checkpoint = UINT64_MAX;
  for (const auto& dumper_pair : _dumpers) {
    uint64_t event_ts = dumper_pair.second->get_event_ts();
    if (event_ts != 0) {
      min_checkpoint = (min_checkpoint > event_ts) ? event_ts : min_checkpoint;
    }
  }
  return min_checkpoint == UINT64_MAX ? 0 : min_checkpoint;
}

bool BinlogDumperManager::can_add_one()
{
  if (_total_limit > 0) {
    uint32_t total_dumpers = dumper_num();
    if (total_dumpers >= _total_limit) {
      OMS_ERROR("The existing dumpers has exceeded threshold: {} > {}", total_dumpers, _total_limit);
      return false;
    }
  }
  /*
   * Note: cpu_limit is the machine CPU used ratio rather than the CPU used ratio of OBI itself
   */
  if (_cpu_limit > 0.0) {
    Cpu* cpu_status = new Cpu(true);
    defer(delete cpu_status);
    if (!collect_cpu(cpu_status)) {
      OMS_ERROR("Collected cpu failed");
      return true;
    }

    float cpu_used_ratio = cpu_status->cpu_used_ratio();
    OMS_INFO("Current used cpu ratio: {}, cpu_limit: {}", cpu_used_ratio, _cpu_limit);
    if (cpu_used_ratio >= _cpu_limit) {
      OMS_ERROR("The CPU usage of this node has reached threshold: {} > {}", cpu_used_ratio, _cpu_limit);
      return false;
    }
  }
  return true;
}

void BinlogDumperManager::update_total_limit(uint16_t total)
{
  OMS_INFO("Begin to update dumper connection limit from {} to {}", _total_limit, total);
  _total_limit = total;
}

void BinlogDumperManager::update_throttle_rps(uint32_t rps)
{
  for (auto& dumper : _dumpers) {
    dumper.second->rate_limiter().update_throttle_rps(rps);
  }
}

void BinlogDumperManager::update_throttle_iops(uint64_t iops)
{
  for (auto& dumper : _dumpers) {
    dumper.second->rate_limiter().update_throttle_iops(iops);
  }
}

}  // namespace oceanbase::binlog
