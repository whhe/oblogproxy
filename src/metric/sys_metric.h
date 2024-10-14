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

#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include "log.h"
#include "model.h"

namespace oceanbase::logproxy {
struct Memory : public Object {
  MODEL_DCL(Memory);
  MODEL_DEF_UINT64(mem_total_size_mb, 0);
  MODEL_DEF_UINT64(mem_used_size_mb, 0);
  MODEL_DEF_FLOAT(mem_used_ratio, 0);

  explicit Memory(bool initialize)
  {}
};

MODEL_DEF(Memory);

struct Cpu : public Object {
  MODEL_DCL(Cpu);
  MODEL_DEF_UINT32(cpu_count, 0);
  MODEL_DEF_FLOAT(cpu_used_ratio, 0);

  explicit Cpu(bool initialize)
  {}
};

MODEL_DEF(Cpu);

struct Disk : public Object {
  MODEL_DCL(Disk);
  // unit MB
  MODEL_DEF_UINT64(disk_total_size_mb, 0);
  // unit MB
  MODEL_DEF_UINT64(disk_used_size_mb, 0);

  MODEL_DEF_FLOAT(disk_used_ratio, 0);
  MODEL_DEF_UINT64(disk_usage_size_process_mb, 0);

  explicit Disk(bool initialize)
  {}
};

MODEL_DEF(Disk);

struct Network : public Object {
  MODEL_DCL(Network);
  MODEL_DEF_UINT64(network_rx_bytes, 0);
  MODEL_DEF_UINT64(network_wx_bytes, 0);

  explicit Network(bool initialize)
  {}
};

MODEL_DEF(Network);

struct Load : public Object {
  MODEL_DCL(Load);
  MODEL_DEF_FLOAT(load_1, 0);
  MODEL_DEF_FLOAT(load_5, 0);
  MODEL_DEF_FLOAT(load_15, 0);

  explicit Load(bool)
  {}
};

MODEL_DEF(Load);

struct ProcessMetric : public Object {
  MODEL_DCL(ProcessMetric);
  MODEL_DEF_UINT64(pid, 0);
  MODEL_DEF_STR(client_id, "");
  MODEL_DEF_OBJECT(memory_status, Memory);
  MODEL_DEF_OBJECT(cpu_status, Cpu);
  MODEL_DEF_OBJECT(disk_status, Disk);
  MODEL_DEF_OBJECT(network_status, Network);
  MODEL_DEF_UINT64(fd_count, 0);

public:
  explicit ProcessMetric(bool initialize);
};

MODEL_DEF(ProcessMetric);

struct ProcessGroupMetric : public Object {
  MODEL_DCL(ProcessGroupMetric);
  MODEL_DEF_LIST(metric_group, ProcessMetric);

  explicit ProcessGroupMetric(bool)
  {}

public:
  std::string item_names[3] = {"logproxy", "oblogreader", "binlog_instance"};
};

MODEL_DEF(ProcessGroupMetric);

struct SysMetric : public Object {
  MODEL_DCL(SysMetric);
  MODEL_DEF_STR(node_id, "");
  MODEL_DEF_STR(ip, "");
  MODEL_DEF_UINT16(port, 0);
  MODEL_DEF_OBJECT(load_status, Load);
  MODEL_DEF_OBJECT(memory_status, Memory);
  MODEL_DEF_OBJECT(cpu_status, Cpu);
  MODEL_DEF_OBJECT(disk_status, Disk);
  MODEL_DEF_OBJECT(network_status, Network);
  MODEL_DEF_OBJECT(process_group_metric, ProcessGroupMetric);

public:
  explicit SysMetric(bool initialize);
};

MODEL_DEF(SysMetric);

/*
 * @param runtime_status
 * @return null
 * @description collect runtime metrics information
 * @date 2022/9/20 16:15
 */
bool collect_metric(SysMetric* metric);

bool collect_process_metric(ProcessGroupMetric* proc_group_metric);

void get_network_stat(Network& network_status);

bool collect_cpu(Cpu* cpu_status);

inline std::mutex g_metric_mutex;
extern SysMetric* g_metric;

}  // namespace oceanbase::logproxy
