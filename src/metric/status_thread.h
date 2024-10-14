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

#include <functional>

#include "thread.h"
#include "sys_metric.h"
#include "timer.h"
#include "cluster/node.h"

namespace oceanbase::logproxy {
class StatusThread : public Thread {
public:
  explicit StatusThread() : _metric(g_metric)
  {
    auto cpu = new Cpu();
    auto load = new Load();
    auto memory = new Memory();
    auto disk = new Disk();
    auto network = new Network();
    auto group_metric = new ProcessGroupMetric();

    _metric->set_memory_status(memory);
    _metric->set_cpu_status(cpu);
    _metric->set_disk_status(disk);
    _metric->set_network_status(network);
    _metric->set_load_status(load);
    _metric->set_process_group_metric(group_metric);

    /*!
     * Initialization attempts to collect current resource information
     */
    collect_metric(_metric);
  }

  void stop() override;

  void register_gauge(const std::string& key, const std::function<int64_t()>& func);

  void mark_sys_metric(stringstream& ss);

  void mark_instance_metric(stringstream& ss);

protected:
  void run() override;

private:
  Timer _timer;

  SysMetric* _metric;
  std::vector<std::pair<std::string, std::function<int64_t()>>> _gauges;
};

}  // namespace oceanbase::logproxy
