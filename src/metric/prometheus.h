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
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include <prometheus/gauge.h>
#include <prometheus/counter.h>
#include <prometheus/histogram.h>
#include <prometheus/info.h>
#include <prometheus/summary.h>
#include "constant.h"

namespace oceanbase::logproxy {
inline std::shared_ptr<prometheus::Registry> g_registry = std::make_shared<prometheus::Registry>();
/*!
 * \brief metirc type
 */
enum PrometheusMetricType {
  BINLOG_CPU_COUNT_TYPE,
  BINLOG_CPU_USED_RATIO_TYPE,
  BINLOG_DISK_TOTAL_SIZE_MB_TYPE,
  BINLOG_DISK_USED_RATIO_TYPE,
  BINLOG_DISK_USED_SIZE_MB_TYPE,
  BINLOG_MEM_TOTAL_SIZE_MB_TYPE,
  BINLOG_MEM_USED_RATIO_TYPE,
  BINLOG_MEM_USED_SIZE_MB_TYPE,
  BINLOG_NETWORK_RX_BYTES_TYPE,
  BINLOG_NETWORK_WX_BYTES_TYPE,
  BINLOG_LOAD1_TYPE,
  BINLOG_LOAD5_TYPE,
  BINLOG_LOAD15_TYPE,
  // instance
  BINLOG_INSTANCE_CPU_COUNT_TYPE,
  BINLOG_INSTANCE_CPU_USED_RATIO_TYPE,
  BINLOG_INSTANCE_DISK_TOTAL_SIZE_MB_TYPE,
  BINLOG_INSTANCE_DISK_USED_RATIO_TYPE,
  BINLOG_INSTANCE_MEM_TOTAL_SIZE_MB_TYPE,
  BINLOG_INSTANCE_MEM_USED_RATIO_TYPE,
  BINLOG_INSTANCE_MEM_USED_SIZE_MB_TYPE,
  BINLOG_INSTANCE_NETWORK_RX_BYTES_TYPE,
  BINLOG_INSTANCE_NETWORK_WX_BYTES_TYPE,
  BINLOG_INSTANCE_FD_COUNT_TYPE,
  // OBCDC Resource Indicators
  BINLOG_INSTANCE_OBCDC_MEM_USED_SIZE_MB_TYPE,

  // Log transformation metrics
  BINLOG_INSTANCE_CONVERT_CHECKPOINT_TYPE,
  BINLOG_INSTANCE_CONVERT_DELAY_TYPE,
  BINLOG_INSTANCE_CONVERT_FETCH_RPS_TYPE,
  BINLOG_INSTANCE_CONVERT_IOPS_TYPE,
  BINLOG_INSTANCE_CONVERT_STORAGE_RPS_TYPE,

  // OBCDC log conversion metric
  BINLOG_INSTANCE_OBCDC_CHECKPOINT_TYPE,
  BINLOG_INSTANCE_OBCDC_DELAY_TYPE,
  BINLOG_INSTANCE_OBCDC_RELEASE_RPS_TYPE,
  BINLOG_INSTANCE_OBCDC_FETCH_CLOG_TRAFFIC_TYPE,
  BINLOG_INSTANCE_OBCDC_NEED_SLOW_DOWN_COUNT_TYPE,
  // viability metric
  BINLOG_INSTANCE_OBCDC_DOWN_TYPE,

  // Subscription metrics
  BINLOG_INSTANCE_DUMP_COUNT_TYPE,
  BINLOG_INSTANCE_DUMP_DELAY_TYPE,
  BINLOG_INSTANCE_DUMP_EVENT_NUM_TYPE,
  BINLOG_INSTANCE_DUMP_EVENT_BYTES_TYPE,
  BINLOG_INSTANCE_DUMP_ERROR_COUNT_TYPE,
  BINLOG_INSTANCE_DUMP_RPS_TYPE,
  BINLOG_INSTANCE_DUMP_HEARTBEAT_RPS_TYPE,
  BINLOG_INSTANCE_DUMP_CHECKPOINT_TYPE,
  BINLOG_INSTANCE_DUMP_IOPS_TYPE,

  // Load related indicator information
  BINLOG_INSTANCE_MASTER_SWITCH_COUNT_TYPE,
  BINLOG_INSTANCE_REPLICATION_NUM_TYPE,
  BINLOG_INSTANCE_NUM_TYPE,
  BINLOG_INSTANCE_DOWN_TYPE,
  BINLOG_INSTANCE_CREATE_TYPE,
  BINLOG_INSTANCE_RELEASE_TYPE,
  BINLOG_INSTANCE_STOP_TYPE,
  BINLOG_CREATE_TYPE,
  BINLOG_RELEASE_TYPE,

  // Counter
  BINLOG_SQL_REQUEST_COUNT_TYPE,
  BINLOG_DUMP_REQUEST_COUNT_TYPE,
  BINLOG_INSTANCE_MASTER_SWITCH_FAILED_COUNT_TYPE,
  BINLOG_INSTANCE_NO_MASTER_COUNT_TYPE,
  BINLOG_INSTANCE_GTID_INCONSISTENT_COUNT_TYPE,
  BINLOG_ALLOCATE_NODE_FAIL_COUNT_TYPE,
  BINLOG_INSTANCE_FAILOVER_FAIL_COUNT_TYPE,
  BINLOG_MANAGER_DOWN_COUNT_TYPE,

  BINLOG_METRIC_END_TYPE
};

struct PrometheusMetricInfo {
  PrometheusMetricType type;
  prometheus::Labels labels;
  bool operator==(const PrometheusMetricInfo& other) const
  {
    return type == other.type && labels == other.labels;
  }
};

/// \brief Combine a hash value with nothing.
/// It's the boundary condition of this serial functions.
///
/// \param seed Not effect.
inline void hash_combine(std::size_t*)
{}

/// \brief Combine the given hash value with another object.
///
/// \param seed The given hash value. It's a input/output parameter.
/// \param value The object that will be combined with the given hash value.
template <typename T>
inline void hash_combine(std::size_t* seed, const T& value)
{
  *seed ^= std::hash<T>{}(value) + 0x9e3779b9 + (*seed << 6) + (*seed >> 2);
}

/// \brief Combine the given hash value with another objects. It's a recursion。
///
/// \param seed The give hash value. It's a input/output parameter.
/// \param value The object that will be combined with the given hash value.
/// \param args The objects that will be combined with the given hash value.
template <typename T, typename... Types>
inline void hash_combine(std::size_t* seed, const T& value, const Types&... args)
{
  hash_combine(seed, value);
  hash_combine(seed, args...);
}

struct PrometheusMetricInfoHasher {
  /// \brief Compute the hash value of a map of labels.
  ///
  /// \param labels The map that will be computed the hash value.
  ///
  /// \returns The hash value of the given labels.
  std::size_t operator()(const PrometheusMetricInfo& labels) const;
};

// Used to record the modification time corresponding to each indicator and perform periodic cleaning operations.
static std::unordered_map<PrometheusMetricInfo, std::pair<prometheus::Counter*, uint64_t>, PrometheusMetricInfoHasher>
    g_counters = {};
inline std::mutex g_counter_mutex;
static std::unordered_map<PrometheusMetricInfo, std::pair<prometheus::Gauge*, uint64_t>, PrometheusMetricInfoHasher>
    g_gauges = {};
inline std::mutex g_gauge_mutex;

static std::unordered_map<PrometheusMetricType, prometheus::Family<prometheus::Gauge>&> g_prometheus_gauge_constant = {
    {BINLOG_CPU_COUNT_TYPE,
        prometheus::BuildGauge().Name(BINLOG_CPU_COUNT.first).Help(BINLOG_CPU_COUNT.second).Register(*g_registry)},
    {BINLOG_CPU_USED_RATIO_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_CPU_USED_RATIO.first)
            .Help(BINLOG_CPU_USED_RATIO.second)
            .Register(*g_registry)},
    {BINLOG_DISK_TOTAL_SIZE_MB_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_DISK_TOTAL_SIZE_MB.first)
            .Help(BINLOG_DISK_TOTAL_SIZE_MB.second)
            .Register(*g_registry)},
    {BINLOG_DISK_USED_RATIO_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_DISK_USED_RATIO.first)
            .Help(BINLOG_DISK_USED_RATIO.second)
            .Register(*g_registry)},
    {BINLOG_DISK_USED_SIZE_MB_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_DISK_USED_SIZE_MB.first)
            .Help(BINLOG_DISK_USED_SIZE_MB.second)
            .Register(*g_registry)},
    {BINLOG_MEM_TOTAL_SIZE_MB_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_MEM_TOTAL_SIZE_MB.first)
            .Help(BINLOG_MEM_TOTAL_SIZE_MB.second)
            .Register(*g_registry)},
    {BINLOG_MEM_USED_RATIO_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_MEM_USED_RATIO.first)
            .Help(BINLOG_MEM_USED_RATIO.second)
            .Register(*g_registry)},
    {BINLOG_MEM_USED_SIZE_MB_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_MEM_USED_SIZE_MB.first)
            .Help(BINLOG_MEM_USED_SIZE_MB.second)
            .Register(*g_registry)},
    {BINLOG_NETWORK_RX_BYTES_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_NETWORK_RX_BYTES.first)
            .Help(BINLOG_NETWORK_RX_BYTES.second)
            .Register(*g_registry)},
    {BINLOG_NETWORK_WX_BYTES_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_NETWORK_WX_BYTES.first)
            .Help(BINLOG_NETWORK_WX_BYTES.second)
            .Register(*g_registry)},
    {BINLOG_LOAD1_TYPE,
        prometheus::BuildGauge().Name(BINLOG_LOAD1.first).Help(BINLOG_LOAD1.second).Register(*g_registry)},
    {BINLOG_LOAD5_TYPE,
        prometheus::BuildGauge().Name(BINLOG_LOAD5.first).Help(BINLOG_LOAD5.second).Register(*g_registry)},
    {BINLOG_LOAD15_TYPE,
        prometheus::BuildGauge().Name(BINLOG_LOAD15.first).Help(BINLOG_LOAD15.second).Register(*g_registry)},
    {BINLOG_INSTANCE_CPU_COUNT_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_CPU_COUNT.first)
            .Help(BINLOG_INSTANCE_CPU_COUNT.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_FD_COUNT_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_FD_COUNT.first)
            .Help(BINLOG_INSTANCE_FD_COUNT.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_CPU_USED_RATIO_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_CPU_USED_RATIO.first)
            .Help(BINLOG_INSTANCE_CPU_USED_RATIO.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_DISK_TOTAL_SIZE_MB_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_DISK_TOTAL_SIZE_MB.first)
            .Help(BINLOG_INSTANCE_DISK_TOTAL_SIZE_MB.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_DISK_USED_RATIO_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_DISK_USED_RATIO.first)
            .Help(BINLOG_INSTANCE_DISK_USED_RATIO.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_MEM_TOTAL_SIZE_MB_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_MEM_TOTAL_SIZE_MB.first)
            .Help(BINLOG_INSTANCE_MEM_TOTAL_SIZE_MB.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_MEM_USED_RATIO_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_MEM_USED_RATIO.first)
            .Help(BINLOG_INSTANCE_MEM_USED_RATIO.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_MEM_USED_SIZE_MB_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_MEM_USED_SIZE_MB.first)
            .Help(BINLOG_INSTANCE_MEM_USED_SIZE_MB.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_NETWORK_RX_BYTES_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_NETWORK_RX_BYTES.first)
            .Help(BINLOG_INSTANCE_NETWORK_RX_BYTES.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_NETWORK_WX_BYTES_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_NETWORK_WX_BYTES.first)
            .Help(BINLOG_INSTANCE_NETWORK_WX_BYTES.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_OBCDC_MEM_USED_SIZE_MB_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_OBCDC_MEM_USED_SIZE_MB.first)
            .Help(BINLOG_INSTANCE_OBCDC_MEM_USED_SIZE_MB.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_CONVERT_CHECKPOINT_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_CONVERT_CHECKPOINT.first)
            .Help(BINLOG_INSTANCE_CONVERT_CHECKPOINT.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_CONVERT_DELAY_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_CONVERT_DELAY.first)
            .Help(BINLOG_INSTANCE_CONVERT_DELAY.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_CONVERT_FETCH_RPS_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_CONVERT_FETCH_RPS.first)
            .Help(BINLOG_INSTANCE_CONVERT_FETCH_RPS.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_CONVERT_IOPS_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_CONVERT_IOPS.first)
            .Help(BINLOG_INSTANCE_CONVERT_IOPS.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_CONVERT_STORAGE_RPS_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_CONVERT_STORAGE_RPS.first)
            .Help(BINLOG_INSTANCE_CONVERT_STORAGE_RPS.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_OBCDC_CHECKPOINT_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_OBCDC_CHECKPOINT.first)
            .Help(BINLOG_INSTANCE_OBCDC_CHECKPOINT.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_OBCDC_DELAY_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_OBCDC_DELAY.first)
            .Help(BINLOG_INSTANCE_OBCDC_DELAY.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_OBCDC_RELEASE_RPS_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_OBCDC_RELEASE_RPS.first)
            .Help(BINLOG_INSTANCE_OBCDC_RELEASE_RPS.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_OBCDC_FETCH_CLOG_TRAFFIC_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_OBCDC_FETCH_CLOG_TRAFFIC.first)
            .Help(BINLOG_INSTANCE_OBCDC_FETCH_CLOG_TRAFFIC.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_OBCDC_NEED_SLOW_DOWN_COUNT_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_OBCDC_NEED_SLOW_DOWN_COUNT.first)
            .Help(BINLOG_INSTANCE_OBCDC_NEED_SLOW_DOWN_COUNT.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_OBCDC_DOWN_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_OBCDC_DOWN.first)
            .Help(BINLOG_INSTANCE_OBCDC_DOWN.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_DUMP_COUNT_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_DUMP_COUNT.first)
            .Help(BINLOG_INSTANCE_DUMP_COUNT.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_DUMP_DELAY_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_DUMP_DELAY.first)
            .Help(BINLOG_INSTANCE_DUMP_DELAY.second)
            .Register(*g_registry)},

    {BINLOG_INSTANCE_DUMP_EVENT_NUM_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_DUMP_EVENT_NUM.first)
            .Help(BINLOG_INSTANCE_DUMP_EVENT_NUM.second)
            .Register(*g_registry)},

    {BINLOG_INSTANCE_DUMP_EVENT_BYTES_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_DUMP_EVENT_BYTES.first)
            .Help(BINLOG_INSTANCE_DUMP_EVENT_BYTES.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_DUMP_RPS_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_DUMP_RPS.first)
            .Help(BINLOG_INSTANCE_DUMP_RPS.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_DUMP_HEARTBEAT_RPS_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_DUMP_HEARTBEAT_RPS.first)
            .Help(BINLOG_INSTANCE_DUMP_HEARTBEAT_RPS.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_DUMP_CHECKPOINT_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_DUMP_CHECKPOINT.first)
            .Help(BINLOG_INSTANCE_DUMP_CHECKPOINT.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_DUMP_IOPS_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_DUMP_IOPS.first)
            .Help(BINLOG_INSTANCE_DUMP_IOPS.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_REPLICATION_NUM_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_REPLICATION_NUM.first)
            .Help(BINLOG_INSTANCE_REPLICATION_NUM.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_NUM_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_NUM.first)
            .Help(BINLOG_INSTANCE_NUM.second)
            .Register(*g_registry)},
    {BINLOG_INSTANCE_DUMP_ERROR_COUNT_TYPE,
        prometheus::BuildGauge()
            .Name(BINLOG_INSTANCE_DUMP_ERROR_COUNT.first)
            .Help(BINLOG_INSTANCE_DUMP_ERROR_COUNT.second)
            .Register(*g_registry)},
    {BINLOG_METRIC_END_TYPE,
        prometheus::BuildGauge().Name(BINLOG_METRIC_END.first).Help(BINLOG_METRIC_END.second).Register(*g_registry)},
};

static std::unordered_map<PrometheusMetricType, prometheus::Family<prometheus::Counter>&>
    g_prometheus_counter_constant = {{BINLOG_SQL_REQUEST_COUNT_TYPE,
                                         prometheus::BuildCounter()
                                             .Name(BINLOG_SQL_REQUEST_COUNT.first)
                                             .Help(BINLOG_SQL_REQUEST_COUNT.second)
                                             .Register(*g_registry)},
        {BINLOG_DUMP_REQUEST_COUNT_TYPE,
            prometheus::BuildCounter()
                .Name(BINLOG_DUMP_REQUEST_COUNT.first)
                .Help(BINLOG_DUMP_REQUEST_COUNT.second)
                .Register(*g_registry)},
        {BINLOG_INSTANCE_MASTER_SWITCH_FAILED_COUNT_TYPE,
            prometheus::BuildCounter()
                .Name(BINLOG_INSTANCE_MASTER_SWITCH_FAILED_COUNT.first)
                .Help(BINLOG_INSTANCE_MASTER_SWITCH_FAILED_COUNT.second)
                .Register(*g_registry)},
        {BINLOG_INSTANCE_NO_MASTER_COUNT_TYPE,
            prometheus::BuildCounter()
                .Name(BINLOG_INSTANCE_NO_MASTER_COUNT.first)
                .Help(BINLOG_INSTANCE_NO_MASTER_COUNT.second)
                .Register(*g_registry)},
        {BINLOG_INSTANCE_GTID_INCONSISTENT_COUNT_TYPE,
            prometheus::BuildCounter()
                .Name(BINLOG_INSTANCE_GTID_INCONSISTENT_COUNT.first)
                .Help(BINLOG_INSTANCE_GTID_INCONSISTENT_COUNT.second)
                .Register(*g_registry)},
        {BINLOG_ALLOCATE_NODE_FAIL_COUNT_TYPE,
            prometheus::BuildCounter()
                .Name(BINLOG_ALLOCATE_NODE_FAIL_COUNT.first)
                .Help(BINLOG_ALLOCATE_NODE_FAIL_COUNT.second)
                .Register(*g_registry)},
        {BINLOG_INSTANCE_DOWN_TYPE,
            prometheus::BuildCounter()
                .Name(BINLOG_INSTANCE_DOWN.first)
                .Help(BINLOG_INSTANCE_DOWN.second)
                .Register(*g_registry)},
        {BINLOG_INSTANCE_CREATE_TYPE,
            prometheus::BuildCounter()
                .Name(BINLOG_INSTANCE_CREATE.first)
                .Help(BINLOG_INSTANCE_CREATE.second)
                .Register(*g_registry)},
        {BINLOG_INSTANCE_RELEASE_TYPE,
            prometheus::BuildCounter()
                .Name(BINLOG_INSTANCE_RELEASE.first)
                .Help(BINLOG_INSTANCE_RELEASE.second)
                .Register(*g_registry)},
        {BINLOG_INSTANCE_STOP_TYPE,
            prometheus::BuildCounter()
                .Name(BINLOG_INSTANCE_STOP.first)
                .Help(BINLOG_INSTANCE_STOP.second)
                .Register(*g_registry)},
        {BINLOG_CREATE_TYPE,
            prometheus::BuildCounter().Name(BINLOG_CREATE.first).Help(BINLOG_CREATE.second).Register(*g_registry)},
        {BINLOG_RELEASE_TYPE,
            prometheus::BuildCounter().Name(BINLOG_RELEASE.first).Help(BINLOG_RELEASE.second).Register(*g_registry)},
        {BINLOG_INSTANCE_MASTER_SWITCH_COUNT_TYPE,
            prometheus::BuildCounter()
                .Name(BINLOG_INSTANCE_MASTER_SWITCH_COUNT.first)
                .Help(BINLOG_INSTANCE_MASTER_SWITCH_COUNT.second)
                .Register(*g_registry)},
        {BINLOG_INSTANCE_FAILOVER_FAIL_COUNT_TYPE,
            prometheus::BuildCounter()
                .Name(BINLOG_INSTANCE_FAILOVER_FAIL_COUNT.first)
                .Help(BINLOG_INSTANCE_FAILOVER_FAIL_COUNT.second)
                .Register(*g_registry)},
        {BINLOG_MANAGER_DOWN_COUNT_TYPE,
            prometheus::BuildCounter()
                .Name(BINLOG_MANAGER_DOWN_COUNT.first)
                .Help(BINLOG_MANAGER_DOWN_COUNT.second)
                .Register(*g_registry)}};

prometheus::Family<prometheus::Gauge>& get_gauge_metric_entry(PrometheusMetricType type);

prometheus::Family<prometheus::Counter>& get_counter_metric_entry(PrometheusMetricType type);

class PrometheusExposer {
public:
  ~PrometheusExposer();

  PrometheusExposer();

  explicit PrometheusExposer(int port);

  explicit PrometheusExposer(std::string ip, int port);

  /*!
   * Resource indicator information used to collect statistics on the node where the service is located
   * @param node_id
   * @param ip
   * @param value
   * @param type
   * @return
   */
  static int mark_binlog_node_resource(
      const std::string& node_id, const std::string& ip, double value, PrometheusMetricType type);

  /*!
   *  Resource indicators used to count OBI
   * @param node_id
   * @param instance_id
   * @param cluster
   * @param tenant
   * @param cluster_id
   * @param tenant_id
   * @param value
   * @param type
   * @return
   */
  static int mark_binlog_instance_resource(const std::string& node_id, const std::string& instance_id,
      const std::string& cluster, const std::string& tenant, const std::string& cluster_id,
      const std::string& tenant_id, double value, PrometheusMetricType type);

  /*!
   * Some non-resource indicators used to count OBI, such as conversion indicator information, etc. Subscription
   * indicators are not included.
   * @param instance_id
   * @param cluster
   * @param tenant
   * @param cluster_id
   * @param tenant_id
   * @param value
   * @param type
   * @return
   */
  static int mark_binlog_instance_metric(const std::string& instance_id, const std::string& cluster,
      const std::string& tenant, const std::string& cluster_id, const std::string& tenant_id, double value,
      PrometheusMetricType type);

  /*!
   * Subscription indicators used to count OBI
   * @param node_id
   * @param instance_id
   * @param cluster
   * @param tenant
   * @param trace_id
   * @param dump_user
   * @param dump_host
   * @param cluster_id
   * @param tenant_id
   * @param value
   * @param type
   * @return
   */
  static int mark_binlog_dump_metric(const std::string& node_id, const std::string& instance_id,
      const std::string& cluster, const std::string& tenant, const std::string& trace_id, const std::string& dump_user,
      const std::string& dump_host, const std::string& cluster_id, const std::string& tenant_id, double value,
      PrometheusMetricType type);

  /*!
   * Subscription indicators used to count OBI
   * @param node_id
   * @param instance_id
   * @param cluster
   * @param tenant
   * @param cluster_id
   * @param tenant_id
   * @param type
   * @param value
   * @return
   */
  static int mark_binlog_dump_error_count_metric(const std::string& node_id, const std::string& instance_id,
      const std::string& cluster, const std::string& tenant, const std::string& cluster_id,
      const std::string& tenant_id, PrometheusMetricType type, double value = 1);

  static void update_guage_modify_ts(PrometheusMetricType type, prometheus::Labels labels, prometheus::Gauge& gauge);

  /*!
   * Used to count the control command indicators received by OBI and OBM, such as some request information, number of
   * master switches, etc.
   * @param node_id
   * @param instance_id
   * @param cluster
   * @param tenant
   * @param cluster_id
   * @param tenant_id
   * @param value
   * @param type
   * @return
   */
  static int mark_binlog_gauge_metric(const std::string& node_id, const std::string& instance_id,
      const std::string& cluster, const std::string& tenant, const std::string& cluster_id,
      const std::string& tenant_id, double value, PrometheusMetricType type);

  static void update_counter_modify_ts(
      PrometheusMetricType type, prometheus::Labels labels, prometheus::Counter& counter);

  static int mark_binlog_counter_metric(const std::string& node_id, const std::string& instance_id,
      const std::string& cluster, const std::string& tenant, const std::string& cluster_id,
      const std::string& tenant_id, PrometheusMetricType type);

  prometheus::Family<prometheus::Gauge>& add_gauge(const std::string& name, const std::string& help);

  prometheus::Family<prometheus::Counter>& add_counter(const std::string& name, const std::string& help);

  prometheus::Family<prometheus::Histogram>& add_histogram(const std::string& name, const std::string& help);

  prometheus::Family<prometheus::Info>& add_info(const std::string& name, const std::string& help);

  prometheus::Family<prometheus::Summary>& add_summary(const std::string& name, const std::string& help);

  template <typename T>
  std::unique_ptr<prometheus::Family<T>>& get_family(const std::string& name);

  static int remove_family(const std::string& name);
  static void clean_up_expired_counter();
  static void clean_up_expired_gauge();

  static int clean_up_expired_indicator_data();

private:
  std::string get_bind_address() const;

  std::string _bind_ip;
  int _bind_port = 2984;
  std::shared_ptr<prometheus::Exposer> _exposer;
};

extern PrometheusExposer* g_exposer;
/*!
 * Initialize prometheus service
 * @param ip
 * @param port
 * @return
 */
int init_prometheus(int port);

void destroy_prometheus();
}  // namespace oceanbase::logproxy
