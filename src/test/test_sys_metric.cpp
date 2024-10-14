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
#include "common.h"
#include "metric/sys_metric.h"

#include <gossip.pb.h>

using namespace oceanbase::logproxy;

TEST(SysMetric, get_network_stat)
{
  Network network_status;
  oceanbase::logproxy::get_network_stat(network_status);
  printf("%lu\n", network_status.network_rx_bytes());
  printf("%lu\n", network_status.network_wx_bytes());
  ASSERT_TRUE(network_status.network_rx_bytes() >= 0 && network_status.network_wx_bytes() >= 0);
}

TEST(ProcessGroupMetric, get_process_group)
{
  ProcessGroupMetric process_group_metric;
  ProcessMetric* process_metric = new ProcessMetric();

  if (process_metric->memory_status() == nullptr) {
    Memory* memory = new Memory();
    process_metric->set_memory_status(memory);
  }

  if (process_metric->cpu_status() == nullptr) {
    Cpu* cpu = new Cpu();
    process_metric->set_cpu_status(cpu);
  }
  if (process_metric->disk_status() == nullptr) {
    Disk* disk = new Disk();
    process_metric->set_disk_status(disk);
  }

  if (process_metric->network_status() == nullptr) {
    Network* network = new Network();
    process_metric->set_network_status(network);
  }
  process_group_metric.metric_group().push_back(process_metric);
  OMS_INFO("process_metric:{}", process_metric->serialize_to_json());
  OMS_INFO("process_group_metric:{}", process_group_metric.serialize_to_json());
  OMS_INFO("process_metric:{}", process_metric->clone()->serialize_to_json());
}

TEST(SysMetric, collect_cpu)
{
  Cpu* cpu = new Cpu(true);
  collect_cpu(cpu);
  printf("%s\n", cpu->serialize_to_json().c_str());
}

TEST(SysMetric, collect_pro_cpu)
{
  Cpu* cpu = new Cpu(true);
  collect_cpu(cpu);
  printf("%s\n", cpu->serialize_to_json().c_str());
}