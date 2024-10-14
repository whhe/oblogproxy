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

#include "status_thread.h"
#include <iomanip>
#include "log.h"
#include "config.h"
#include "prometheus.h"
#include "timer.h"

namespace oceanbase {
namespace logproxy {

void StatusThread::stop()
{
  Thread::stop();
}

void StatusThread::register_gauge(const std::string& key, const std::function<int64_t()>& func)
{
  _gauges.emplace_back(key, func);
}

void StatusThread::run()
{
  std::stringstream ss;
  while (is_run()) {
    if (!_gauges.empty()) {
      ss.str("");
      ss << "COUNTS:";
      for (auto& entry : _gauges) {
        ss << "[" << entry.first << ":" << entry.second() << "]";
      }
      OMS_STREAM_INFO << ss.str();
    }

    if (!collect_metric(_metric)) {
      OMS_STREAM_ERROR << "Failed to collect metric";
      continue;
    }

    mark_sys_metric(ss);
    mark_instance_metric(ss);
    PrometheusExposer::clean_up_expired_indicator_data();
    _timer.sleep(Config::instance().metric_interval_s.val() * 1000000);
  }
}
void StatusThread::mark_instance_metric(std::stringstream& ss)
{
  for (auto iterator = _metric->process_group_metric()->metric_group().get_items().begin();
       iterator != _metric->process_group_metric()->metric_group().get_items().end();
       iterator++) {
    ProcessMetric* p_process_metric = ((ProcessMetric*)(*iterator));
    ss.str("");
    ss << "METRICS:";
    ss << "[CLIENT_ID:" << p_process_metric->client_id() << "]";
    ss << "[PID:" << p_process_metric->pid() << "]";
    ss << "[MEM:" << p_process_metric->memory_status()->mem_used_size_mb() << "/"
       << p_process_metric->memory_status()->mem_total_size_mb() << "MB," << std::setprecision(4)
       << p_process_metric->memory_status()->mem_used_ratio() * 100 << "%]"
       << "[UDISK:" << p_process_metric->disk_status()->disk_usage_size_process_mb() << "/"
       << p_process_metric->disk_status()->disk_total_size_mb() << "MB," << std::setprecision(4)
       << p_process_metric->disk_status()->disk_used_ratio() * 100 << "%]"
       << "[CPU:" << p_process_metric->cpu_status()->cpu_count() << "," << std::setprecision(4)
       << p_process_metric->cpu_status()->cpu_used_ratio() * 100 << "%]"
       << "[NETIO:" << p_process_metric->network_status()->network_rx_bytes() / 1024 << "KB/s,"
       << _metric->network_status()->network_wx_bytes() / 1024 << "KB/s]";
    OMS_INFO(ss.str());
  }
}

void StatusThread::mark_sys_metric(std::stringstream& ss)
{
  ss.str("");
  ss << "METRICS:";
  ss << "[MEM:" << _metric->memory_status()->mem_used_size_mb() << "/" << _metric->memory_status()->mem_total_size_mb()
     << "MB," << std::setprecision(4) << _metric->memory_status()->mem_used_ratio() * 100 << "%]"
     << "[UDISK:" << _metric->disk_status()->disk_used_size_mb() << "/" << _metric->disk_status()->disk_total_size_mb()
     << "MB," << std::setprecision(4) << _metric->disk_status()->disk_used_ratio() * 100 << "%]"
     << "[CPU:" << _metric->cpu_status()->cpu_count() << "," << std::setprecision(4)
     << _metric->cpu_status()->cpu_used_ratio() * 100 << "%]"
     << "[LOAD1,5:" << _metric->load_status()->load_1() * 100 << "%," << _metric->load_status()->load_5() * 100 << "%]"
     << "[NETIO:" << _metric->network_status()->network_rx_bytes() / 1024 << "KB/s,"
     << _metric->network_status()->network_wx_bytes() / 1024 << "KB/s]";
  OMS_INFO(ss.str());

  PrometheusExposer::mark_binlog_node_resource(
      _metric->node_id(), _metric->ip(), _metric->memory_status()->mem_total_size_mb(), BINLOG_MEM_TOTAL_SIZE_MB_TYPE);
  PrometheusExposer::mark_binlog_node_resource(
      _metric->node_id(), _metric->ip(), _metric->memory_status()->mem_used_size_mb(), BINLOG_MEM_USED_SIZE_MB_TYPE);
  PrometheusExposer::mark_binlog_node_resource(
      _metric->node_id(), _metric->ip(), _metric->memory_status()->mem_used_ratio(), BINLOG_MEM_USED_RATIO_TYPE);

  PrometheusExposer::mark_binlog_node_resource(
      _metric->node_id(), _metric->ip(), _metric->cpu_status()->cpu_count(), BINLOG_CPU_COUNT_TYPE);
  PrometheusExposer::mark_binlog_node_resource(
      _metric->node_id(), _metric->ip(), _metric->cpu_status()->cpu_used_ratio(), BINLOG_CPU_USED_RATIO_TYPE);

  PrometheusExposer::mark_binlog_node_resource(
      _metric->node_id(), _metric->ip(), _metric->load_status()->load_1(), BINLOG_LOAD1_TYPE);
  PrometheusExposer::mark_binlog_node_resource(
      _metric->node_id(), _metric->ip(), _metric->load_status()->load_5(), BINLOG_LOAD5_TYPE);
  PrometheusExposer::mark_binlog_node_resource(
      _metric->node_id(), _metric->ip(), _metric->load_status()->load_15(), BINLOG_LOAD15_TYPE);

  PrometheusExposer::mark_binlog_node_resource(
      _metric->node_id(), _metric->ip(), _metric->network_status()->network_rx_bytes(), BINLOG_NETWORK_RX_BYTES_TYPE);
  PrometheusExposer::mark_binlog_node_resource(
      _metric->node_id(), _metric->ip(), _metric->network_status()->network_wx_bytes(), BINLOG_NETWORK_WX_BYTES_TYPE);

  PrometheusExposer::mark_binlog_node_resource(
      _metric->node_id(), _metric->ip(), _metric->disk_status()->disk_total_size_mb(), BINLOG_DISK_TOTAL_SIZE_MB_TYPE);
  PrometheusExposer::mark_binlog_node_resource(
      _metric->node_id(), _metric->ip(), _metric->disk_status()->disk_used_size_mb(), BINLOG_DISK_USED_SIZE_MB_TYPE);
  PrometheusExposer::mark_binlog_node_resource(
      _metric->node_id(), _metric->ip(), _metric->disk_status()->disk_used_ratio(), BINLOG_DISK_USED_RATIO_TYPE);
}

}  // namespace logproxy
}  // namespace oceanbase
