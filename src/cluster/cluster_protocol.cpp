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

#include "cluster_protocol.h"
#include "timer.h"

namespace oceanbase::logproxy {
ClusterProtocol::ClusterProtocol(
    ClusterConfig* cluster_config, TaskExecutorCb& task_executor_cb, InstanceFailoverCb& failover_cb)
    : _cluster_config(cluster_config), _task_executor_cb(task_executor_cb), _instance_failover_cb(failover_cb)
{
  _node_info.set_ip(cluster_config->node_ip());
  _node_info.set_port(cluster_config->server_port());
  _node_info.set_id(cluster_config->node_id());
  _node_info.set_node_config(cluster_config->node_config());
  _node_info.set_region(cluster_config->region());
  _node_info.set_zone(cluster_config->zone());
  _node_info.set_state(State::ONLINE);
  _node_info.set_last_modify(Timer::now_s());
  _node_info.set_metric(new SysMetric());
}

ClusterProtocol::~ClusterProtocol()
{
  delete _cluster_config;
}

const Node& ClusterProtocol::get_node_info()
{
  return _node_info;
}

ClusterConfig* ClusterProtocol::get_cluster_config()
{
  return _cluster_config;
}

TaskExecutorCb& ClusterProtocol::get_task_executor_cb()
{
  return _task_executor_cb;
}

InstanceFailoverCb& ClusterProtocol::get_instance_failover_cb()
{
  return _instance_failover_cb;
}

ThreadPoolExecutor& ClusterProtocol::get_thread_executor()
{
  return _thread_executor;
}

}  // namespace oceanbase::logproxy
