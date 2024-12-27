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

#include "env.h"
#include "common_util.h"
#include "instance_tasks.h"

namespace oceanbase::binlog {
ConnectionManager* g_connection_manager = nullptr;
BinlogDumperManager* g_dumper_manager = nullptr;
ThreadPoolExecutor* g_executor = nullptr;
ThreadPoolExecutor* g_purge_binlog_executor = nullptr;
ThreadPoolExecutor* g_sql_executor = nullptr;
shared_ptr<Disruptor::disruptor<ReleaseEvent>> g_disruptor = nullptr;
SysVar* g_sys_var = nullptr;
TcpPortPool* g_tcp_port_pool = nullptr;
ClusterProtocol* g_cluster = nullptr;

BinlogIndexManager* g_index_manager = nullptr;
GtidManager* g_gtid_manager = nullptr;

GeometryConverter* g_gis_converter = nullptr;

std::vector<shared_ptr<Disruptor::IWorkHandler<ReleaseEvent>>> g_release_event_handler;
shared_ptr<Disruptor::RoundRobinThreadAffinedTaskScheduler> task_scheduler = nullptr;

int init_cluster_config(ClusterConfig* cluster_config);

int env_init(uint32_t nof_work_threads, uint32_t sql_work_threads, uint16_t start_tcp_port, uint16_t reserved_ports_num)
{
  g_connection_manager = new ConnectionManager{};
  g_executor = new ThreadPoolExecutor{nof_work_threads};
  g_sql_executor = new ThreadPoolExecutor{sql_work_threads};
  g_sys_var = new SysVar{};

  auto* cluster_config = new ClusterConfig();
  if (init_cluster_config(cluster_config) != OMS_OK) {
    OMS_ERROR("Failed to initialize cluster configuration");
    return OMS_FAILED;
  }
  TaskExecutorCb task_executor_cb = TaskCallback::task_executor;
  InstanceFailoverCb recover_cb = InstanceFailoverCallback::instance_failover;
  if (strcmp(s_config.cluster_protocol.val().c_str(), "database") == 0) {
    g_cluster = logproxy::ClusterProtocolFactory::create(cluster_config, DATABASE, task_executor_cb, recover_cb);
  } else if (strcmp(s_config.cluster_protocol.val().c_str(), "gossip") == 0) {
    g_cluster = logproxy::ClusterProtocolFactory::create(cluster_config, GOSSIP, task_executor_cb, recover_cb);
  } else {
    g_cluster = logproxy::ClusterProtocolFactory::create(cluster_config, STANDALONE, task_executor_cb, recover_cb);
  }

  g_tcp_port_pool = new TcpPortPool{start_tcp_port, reserved_ports_num};
  if (OMS_OK != g_tcp_port_pool->init()) {
    return OMS_FAILED;
  }
  return evthread_use_pthreads();
}

int init_cluster_config(ClusterConfig* cluster_config)
{
  std::string node_local_file = logproxy::Config::instance().binlog_log_bin_basename.val() + "/" + NODE_LOCAL_FILENAME;
  if (FsUtil::exist(node_local_file)) {
    std::string node_id;
    if (!FsUtil::read_file(node_local_file, node_id)) {
      OMS_ERROR("Failed to read node.local");
      return OMS_FAILED;
    }
    cluster_config->set_node_id(node_id);
  } else {
    cluster_config->set_node_id(CommonUtils::generate_trace_id());
    FsUtil::write_file(node_local_file, cluster_config->node_id());
  }
  cluster_config->set_node_ip(s_config.node_ip.val());
  cluster_config->set_server_port(s_config.service_port.val());
  cluster_config->set_database_ip(s_config.database_ip.val());
  cluster_config->set_database_port(s_config.database_port.val());
  cluster_config->set_database_name(s_config.database_name.val());
  cluster_config->set_database_properties(s_config.database_properties.val());
  cluster_config->set_min_pool_size(s_config.min_pool_size.val());
  cluster_config->set_user(s_config.user.val());
  cluster_config->set_password(s_config.password.val());
  cluster_config->set_push_pull_interval_us(s_config.push_pull_interval_us.val());
  cluster_config->set_task_interval_us(s_config.task_interval_us.val());
  return OMS_OK;
};

void env_deInit()
{
  delete g_executor;
  delete g_connection_manager;
  delete g_sys_var;
  delete g_sql_executor;
  delete g_tcp_port_pool;
  delete g_cluster;
}

int instance_env_init(uint32_t nof_work_threads, uint32_t obi_convert_work_threads, uint32_t obi_purge_binlog_threads)
{
  g_connection_manager = new ConnectionManager{};
  g_dumper_manager = s_config.enable_dumper_cpu_precheck.val()
                         ? new BinlogDumperManager(s_meta.binlog_config()->throttle_dump_conn(),
                               (s_config.node_cpu_limit_threshold_percent.val() * 0.01))
                         : new BinlogDumperManager(s_meta.binlog_config()->throttle_dump_conn());
  g_executor = new ThreadPoolExecutor{nof_work_threads};
  g_sys_var = new SysVar{};
  g_index_manager = new BinlogIndexManager(BINLOG_INDEX_NAME);
  g_gtid_manager = new GtidManager(GTID_SEQ_FILENAME);
  g_gis_converter = new GeometryConverter{};
  g_purge_binlog_executor = new ThreadPoolExecutor{obi_purge_binlog_threads};

  task_scheduler = std::make_shared<Disruptor::RoundRobinThreadAffinedTaskScheduler>();
  for (int64_t i = 0; i < s_config.binlog_release_parallel_size.val(); i++) {
    g_release_event_handler.push_back(std::make_shared<ReleaseEventHandler>());
  }

  if (s_config.binlog_release_ring_buffer_size.val() < 1) {
    OMS_ERROR("binlog_release_ring_buffer_size must not be less than 1");
    return OMS_FAILED;
  }

  if (!Disruptor::Util::isPowerOf2(s_config.binlog_release_ring_buffer_size.val())) {
    OMS_ERROR("binlog_release_ring_buffer_size must be a power of 2");
    return OMS_FAILED;
  }
  g_disruptor = std::make_shared<Disruptor::disruptor<ReleaseEvent>>(
      create_release_event, s_config.binlog_release_ring_buffer_size.val(), task_scheduler);
  g_disruptor->handleEventsWithWorkerPool(g_release_event_handler);
  task_scheduler->start(s_config.binlog_release_thread_size.val());
  g_disruptor->start();
  return evthread_use_pthreads();
}

void instance_env_deInit()
{
  g_disruptor->shutdown();
  task_scheduler->stop();
  delete g_connection_manager;
  delete g_dumper_manager;
  delete g_executor;
  delete g_sys_var;
  delete g_index_manager;
  delete g_gtid_manager;
  delete g_gis_converter;
  // delete g_column_convert_executor;
  delete g_purge_binlog_executor;
}
}  // namespace oceanbase::binlog
