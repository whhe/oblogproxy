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

#include "prometheus.h"

#include "communication/io.h"
#include "log.h"
#include "timer.h"

namespace oceanbase::logproxy {

PrometheusExposer::PrometheusExposer(int port)
{
  _bind_port = port;
  _bind_ip = "0.0.0.0";
  _exposer = std::make_shared<prometheus::Exposer>(get_bind_address());

  // Register the registry into the exposer so that Prometheus can crawl it
  _exposer->RegisterCollectable(g_registry);
}

PrometheusExposer::~PrometheusExposer() = default;

int PrometheusExposer::mark_binlog_node_resource(
    const std::string& node_id, const std::string& ip, double value, PrometheusMetricType type)
{
  if (type > BINLOG_LOAD15_TYPE) {
    OMS_ERROR("[prometheus binlog node]Current indicator type: {} does not belong to node indicators", type);
    return OMS_FAILED;
  }
  prometheus::Labels labels;
  labels["host_name"] = node_id;
  labels["ip"] = ip;
  auto& gauge = get_gauge_metric_entry(type).Add(labels);
  gauge.Set(value);
  update_guage_modify_ts(type, labels, gauge);
  return OMS_OK;
}

int PrometheusExposer::mark_binlog_instance_resource(const std::string& node_id, const std::string& instance_id,
    const std::string& cluster, const std::string& tenant, const std::string& cluster_id, const std::string& tenant_id,
    double value, PrometheusMetricType type)
{
  if (type <= BINLOG_LOAD15_TYPE || type > BINLOG_INSTANCE_OBCDC_MEM_USED_SIZE_MB_TYPE) {
    OMS_ERROR("[prometheus binlog instance]Current indicator type: {} does not belong to instance indicators", type);
    return OMS_FAILED;
  }
  prometheus::Labels labels;
  labels["host_name"] = node_id;
  labels["instance_id"] = instance_id;
  labels["ob_cluster_name"] = cluster;
  labels["tenant_name"] = tenant;
  if (!cluster_id.empty()) {
    labels[OB_CLUSTER_ID] = cluster_id;
  }

  if (!tenant_id.empty()) {
    labels[OB_TENANT_ID] = tenant_id;
  }
  auto& gauge = get_gauge_metric_entry(type).Add(labels);
  gauge.Set(value);
  update_guage_modify_ts(type, labels, gauge);
  return OMS_OK;
}

int PrometheusExposer::mark_binlog_instance_metric(const std::string& instance_id, const std::string& cluster,
    const std::string& tenant, const std::string& cluster_id, const std::string& tenant_id, double value,
    PrometheusMetricType type)
{
  if (type <= BINLOG_INSTANCE_OBCDC_MEM_USED_SIZE_MB_TYPE || type >= BINLOG_INSTANCE_DUMP_COUNT_TYPE) {
    OMS_ERROR("[prometheus binlog instance]Current indicator type: {} does not belong to instance indicators", type);
    return OMS_FAILED;
  }
  prometheus::Labels labels;
  labels["instance_id"] = instance_id;
  labels["ob_cluster_name"] = cluster;
  labels["tenant_name"] = tenant;
  if (!cluster_id.empty()) {
    labels[OB_CLUSTER_ID] = cluster_id;
  }

  if (!tenant_id.empty()) {
    labels[OB_TENANT_ID] = tenant_id;
  }
  auto& gauge = get_gauge_metric_entry(type).Add(labels);
  gauge.Set(value);
  update_guage_modify_ts(type, labels, gauge);

  return OMS_OK;
}

int PrometheusExposer::mark_binlog_dump_metric(const std::string& node_id, const std::string& instance_id,
    const std::string& cluster, const std::string& tenant, const std::string& trace_id, const std::string& dump_user,
    const std::string& dump_host, const std::string& cluster_id, const std::string& tenant_id, double value,
    PrometheusMetricType type)
{
  if (type < BINLOG_INSTANCE_DUMP_COUNT_TYPE || type > BINLOG_INSTANCE_DUMP_IOPS_TYPE) {
    OMS_ERROR("[prometheus binlog instance]Current indicator type: {} does not belong to instance indicators", type);
    return OMS_FAILED;
  }
  prometheus::Labels labels;
  labels["host_name"] = node_id;
  labels["instance_id"] = instance_id;
  labels["ob_cluster_name"] = cluster;
  labels["tenant_name"] = tenant;
  labels["trace_id"] = trace_id;
  if (!dump_user.empty()) {
    labels["user"] = dump_user;
  }

  if (!dump_host.empty()) {
    labels["host"] = dump_host;
  }

  if (!cluster_id.empty()) {
    labels[OB_CLUSTER_ID] = cluster_id;
  }

  if (!tenant_id.empty()) {
    labels[OB_TENANT_ID] = tenant_id;
  }
  auto& gauge = get_gauge_metric_entry(type).Add(labels);
  gauge.Set(value);
  update_guage_modify_ts(type, labels, gauge);
  return OMS_OK;
}

int PrometheusExposer::mark_binlog_dump_error_count_metric(const std::string& node_id, const std::string& instance_id,
    const std::string& cluster, const std::string& tenant, const std::string& cluster_id, const std::string& tenant_id,
    PrometheusMetricType type, double value)
{
  prometheus::Labels labels;
  labels["host_name"] = node_id;
  labels["instance_id"] = instance_id;
  labels["ob_cluster_name"] = cluster;
  labels["tenant_name"] = tenant;
  if (!cluster_id.empty()) {
    labels[OB_CLUSTER_ID] = cluster_id;
  }

  if (!tenant_id.empty()) {
    labels[OB_TENANT_ID] = tenant_id;
  }
  auto& gauge = get_gauge_metric_entry(type).Add(labels);
  gauge.Set(value);
  update_guage_modify_ts(type, labels, gauge);
  return OMS_OK;
}

void PrometheusExposer::update_guage_modify_ts(
    PrometheusMetricType type, prometheus::Labels labels, prometheus::Gauge& gauge)
{
  std::unique_lock<std::mutex> lock_guard(g_gauge_mutex);
  g_gauges.insert_or_assign(PrometheusMetricInfo{type, labels}, std::make_pair(&gauge, Timer::now_s()));
}
int PrometheusExposer::mark_binlog_gauge_metric(const std::string& node_id, const std::string& instance_id,
    const std::string& cluster, const std::string& tenant, const std::string& cluster_id, const std::string& tenant_id,
    double value, PrometheusMetricType type)
{
  if (g_prometheus_gauge_constant.find(type) == g_prometheus_gauge_constant.end()) {
    OMS_ERROR("[prometheus control metric]Current indicator type: {} does not belong to control metric", type);
    return OMS_FAILED;
  }
  prometheus::Labels labels;
  if (!node_id.empty()) {
    labels["host_name"] = node_id;
  }

  if (!instance_id.empty()) {
    labels["instance_id"] = instance_id;
  }
  if (!cluster.empty()) {
    labels["ob_cluster_name"] = cluster;
  }
  if (!tenant.empty()) {
    labels["tenant_name"] = tenant;
  }

  if (!cluster_id.empty()) {
    labels[OB_CLUSTER_ID] = cluster_id;
  }

  if (!tenant_id.empty()) {
    labels[OB_TENANT_ID] = tenant_id;
  }
  auto& gauge = get_gauge_metric_entry(type).Add(labels);
  gauge.Set(value);
  update_guage_modify_ts(type, labels, gauge);
  return OMS_OK;
}

void PrometheusExposer::update_counter_modify_ts(
    PrometheusMetricType type, prometheus::Labels labels, prometheus::Counter& counter)
{
  std::unique_lock<std::mutex> lock(g_counter_mutex);
  g_counters.insert_or_assign(PrometheusMetricInfo{type, labels}, std::make_pair(&counter, Timer::now_s()));
}
int PrometheusExposer::mark_binlog_counter_metric(const std::string& node_id, const std::string& instance_id,
    const std::string& cluster, const std::string& tenant, const std::string& cluster_id, const std::string& tenant_id,
    PrometheusMetricType type)
{
  if (g_prometheus_counter_constant.find(type) == g_prometheus_counter_constant.end()) {
    OMS_ERROR("[prometheus counter]Current indicator type: {} does not belong to counter indicators", type);
    return OMS_FAILED;
  }
  prometheus::Labels labels;
  if (!node_id.empty()) {
    labels["host_name"] = node_id;
  }

  if (!instance_id.empty()) {
    labels["instance_id"] = instance_id;
  }
  labels["ob_cluster_name"] = cluster;
  labels["tenant_name"] = tenant;

  if (!cluster_id.empty()) {
    labels[OB_CLUSTER_ID] = cluster_id;
  }

  if (!tenant_id.empty()) {
    labels[OB_TENANT_ID] = tenant_id;
  }
  auto& counter = get_counter_metric_entry(type).Add(labels);
  counter.Increment();
  update_counter_modify_ts(type, labels, counter);
  return OMS_OK;
}

prometheus::Family<prometheus::Gauge>& PrometheusExposer::add_gauge(const std::string& name, const std::string& help)
{
  return prometheus::BuildGauge().Name(name).Help(help).Register(*g_registry);
}

prometheus::Family<prometheus::Counter>& PrometheusExposer::add_counter(
    const std::string& name, const std::string& help)
{
  return prometheus::BuildCounter().Name(name).Help(help).Register(*g_registry);
}

prometheus::Family<prometheus::Histogram>& PrometheusExposer::add_histogram(
    const std::string& name, const std::string& help)
{
  return prometheus::BuildHistogram().Name(name).Help(help).Register(*g_registry);
}

prometheus::Family<prometheus::Info>& PrometheusExposer::add_info(const std::string& name, const std::string& help)
{
  return prometheus::BuildInfo().Name(name).Help(help).Register(*g_registry);
}

prometheus::Family<prometheus::Summary>& PrometheusExposer::add_summary(
    const std::string& name, const std::string& help)
{
  return prometheus::BuildSummary().Name(name).Help(help).Register(*g_registry);
}

template <typename T>
std::unique_ptr<prometheus::Family<T>>& PrometheusExposer::get_family(const std::string& name)
{
  return nullptr;
}

int PrometheusExposer::remove_family(const std::string& name)
{
  return OMS_OK;
}

void PrometheusExposer::clean_up_expired_counter()
{
  std::unique_lock<std::mutex> lock_guard(g_counter_mutex);
  for (auto item = g_counters.begin(); item != g_counters.end();) {
    if (Timer::now_s() - item->second.second > Config::instance().prometheus_unused_metric_clear_interval_s.val()) {
      get_counter_metric_entry(item->first.type).Remove(item->second.first);
      item = g_counters.erase(item);
    } else {
      ++item;
    }
  }
}
void PrometheusExposer::clean_up_expired_gauge()
{
  std::unique_lock<std::mutex> lock_guard(g_gauge_mutex);
  for (auto item = g_gauges.begin(); item != g_gauges.end();) {
    if (Timer::now_s() - item->second.second > Config::instance().prometheus_unused_metric_clear_interval_s.val()) {
      get_gauge_metric_entry(item->first.type).Remove(item->second.first);
      item = g_gauges.erase(item);
    } else {
      ++item;
    }
  }
}

int PrometheusExposer::clean_up_expired_indicator_data()
{
  clean_up_expired_counter();

  clean_up_expired_gauge();
  return OMS_OK;
}

std::string PrometheusExposer::get_bind_address() const
{
  return _bind_ip + ":" + std::to_string(_bind_port);
}

PrometheusExposer::PrometheusExposer()
{}

PrometheusExposer::PrometheusExposer(std::string ip, int port)
{
  _bind_port = port;
  _bind_ip = ip;
  _exposer = std::make_shared<prometheus::Exposer>(get_bind_address());

  // Register the registry into the exposer so that Prometheus can crawl it
  _exposer->RegisterCollectable(g_registry);
}

std::size_t PrometheusMetricInfoHasher::operator()(const PrometheusMetricInfo& labels) const
{
  std::size_t seed = 0;
  for (auto& label : labels.labels) {
    hash_combine(&seed, label.first, label.second, labels.type);
  }

  return seed;
}
prometheus::Family<prometheus::Gauge>& get_gauge_metric_entry(PrometheusMetricType type)
{
  const auto iter = g_prometheus_gauge_constant.find(type);
  if (iter != g_prometheus_gauge_constant.end()) {
    return iter->second;
  }
  return g_prometheus_gauge_constant.at(BINLOG_METRIC_END_TYPE);
}

prometheus::Family<prometheus::Counter>& get_counter_metric_entry(PrometheusMetricType type)
{
  const auto iter = g_prometheus_counter_constant.find(type);
  if (iter != g_prometheus_counter_constant.end()) {
    return iter->second;
  }
  return g_prometheus_counter_constant.at(BINLOG_METRIC_END_TYPE);
}

PrometheusExposer* g_exposer = nullptr;

int init_prometheus(int port)
{
  if (port_in_use(port)) {
    OMS_ERROR("Prometheus port [{}] is occupied", port);
    return OMS_FAILED;
  }
  g_exposer = new PrometheusExposer(port);
  return OMS_OK;
}

void destroy_prometheus()
{
  if (g_exposer != nullptr) {
    delete g_exposer;
  }
}

}  // namespace oceanbase::logproxy
