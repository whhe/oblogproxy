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

#include "binlog_manager.h"

#include <fcntl.h>

#include "log.h"
#include "env.h"
#include "event_dispatch.h"
#include "metric/status_thread.h"
#include "gtid_inspector.h"
namespace oceanbase::binlog {
int init_global_config();

void BinlogManager::init_metric_config()
{
  g_metric->set_ip(s_config.node_ip.val());
  g_metric->set_port(s_config.service_port.val());
  g_metric->set_node_id(g_cluster->get_node_info().id());
}

int BinlogManager::run_foreground()
{
  FsUtil::mkdir(s_config.binlog_log_bin_basename.val());

  uint16_t sys_var_listen_port = s_config.service_port.val();
  uint32_t sys_var_nof_work_threads = s_config.binlog_nof_work_threads.val();
  uint32_t sys_var_sql_work_threads = s_config.binlog_sql_work_threads.val();
  uint16_t start_tcp_port = s_config.start_tcp_port.val();
  uint16_t reserved_ports_num = s_config.reserved_ports_num.val();
  if (env_init(sys_var_nof_work_threads, sys_var_sql_work_threads, start_tcp_port, reserved_ports_num) != OMS_OK) {
    OMS_ERROR("Failed to start OceanBase binlog server");
    return OMS_FAILED;
  }

  if (OMS_OK != init_global_config()) {
    OMS_ERROR("Failed to init global config");
    return OMS_FAILED;
  }

  struct event_base* ev_base = event_base_new();
  int error_no = 0;
  struct evconnlistener* listener =
      evconnlistener_new(ev_base, sys_var_listen_port, on_new_obm_connection_cb, error_no);
  if (listener == nullptr) {
    OMS_ERROR("Failed to start OceanBase binlog server on port: {}, error: {}",
        sys_var_listen_port,
        logproxy::system_err(error_no));
    event_base_free(ev_base);
    return OMS_FAILED;
  }

  fcntl(evconnlistener_get_fd(listener), F_SETFD, FD_CLOEXEC);
  OMS_INFO("Start OceanBase binlog server on port: {}", sys_var_listen_port);
  logproxy::StatusThread status_thread;
  init_metric_config();
  if (s_config.metric_enable.val()) {
    OMS_INFO("Start indicator statistics");
    status_thread.start();
    status_thread.detach();
  }

  if (s_config.cluster_mode.val()) {
    if (g_cluster->register_node() != OMS_OK) {
      OMS_ERROR("Failed to register node");
      event_base_free(ev_base);
      return OMS_FAILED;
    }
  }

  GtidConsistencyInspector gtid_inspector;
  if (s_config.enable_gtid_inspector.val()) {
    gtid_inspector.start();
    gtid_inspector.detach();
  }

  event_base_dispatch(ev_base);
  OMS_INFO("Stop OceanBase binlog server");

  evconnlistener_free(listener);
  event_base_free(ev_base);
  env_deInit();

  return OMS_OK;
}

int init_global_config()
{
  std::map<std::string, std::string> configs;
  if (OMS_OK != g_cluster->query_configs_by_granularity(configs, Granularity::G_GLOBAL, "")) {
    OMS_ERROR("Failed to query global configs from metadb.");
    return OMS_FAILED;
  }

  s_config.init(configs);
  OMS_INFO("Success init config according to global configs:\n {}", s_config.to_string(false, true));
  return OMS_OK;
}

}  // namespace oceanbase::binlog
