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

#include "connection_manager.h"
#include "sys_var.h"
#include "thread_pool_executor.h"
#include "metric/sys_metric.h"
#include "binlog_state_machine.h"
#include "tcp_port_pool.h"
#include "cluster/cluster.h"
#include "binlog_index.h"
#include "gtid_manager.h"
#include "selection_strategy.h"
#include "cluster/cluster_config.h"
#include "cluster/cluster_protocol.h"
#include "binlog-instance/binlog_dumper_manager.h"
#include "common_util.h"
#include "release_event_helper.h"

namespace oceanbase::binlog {
static logproxy::Config& s_config = logproxy::Config::instance();
extern ConnectionManager* g_connection_manager;
extern BinlogDumperManager* g_dumper_manager;
extern logproxy::ThreadPoolExecutor* g_executor;
extern logproxy::ThreadPoolExecutor* g_purge_binlog_executor;
extern logproxy::ThreadPoolExecutor* g_sql_executor;
extern shared_ptr<Disruptor::disruptor<ReleaseEvent>> g_disruptor;
extern SysVar* g_sys_var;
extern TcpPortPool* g_tcp_port_pool;
extern ClusterProtocol* g_cluster;

// only for binlog instance
static InstanceMeta& s_meta = InstanceMeta::instance();

extern BinlogIndexManager* g_index_manager;
extern GtidManager* g_gtid_manager;
extern GeometryConverter* g_gis_converter;

extern std::vector<shared_ptr<Disruptor::IWorkHandler<ReleaseEvent>>> g_release_event_handler;
extern shared_ptr<Disruptor::RoundRobinThreadAffinedTaskScheduler> task_scheduler;
int env_init(
    uint32_t nof_work_threads, uint32_t sql_work_threads, uint16_t start_tcp_port, uint16_t reserved_ports_num);

void env_deInit();

int instance_env_init(uint32_t nof_work_threads, uint32_t obi_convert_work_threads, uint32_t obi_purge_binlog_threads);

void instance_env_deInit();
}  // namespace oceanbase::binlog
