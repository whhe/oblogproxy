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

#include <string>

namespace oceanbase {
namespace logproxy {
const static std::string PATH_CPU_SET = "/sys/fs/cgroup/cpuset";
const static std::string PATH_CPU = "/sys/fs/cgroup/cpu";
const static std::string PATH_MEMORY = "/sys/fs/cgroup/memory";
const static std::string PATH_BLKIO = "/sys/fs/cgroup/blkio";
const static std::string PATH_CPU_ACCT = "/sys/fs/cgroup/cpuacct";
const static std::string PATH_SYS_PROC = "/proc";
const static int64_t MEM_DEFAULT = 9223372036854771712L;
const static int64_t UNIT_GB = 1024 * 1024 * 1024L;
const static int64_t UNIT_MB = 1024 * 1024L;
const static int64_t UNIT_KB = 1024L;

const static std::string NEWLINE = "\\r?\\n";
const static std::string PIPE = "|";
const static std::string SPACE = " ";
const static std::string COMMA = ",";
const static std::string HYPHEN = "-";
const static std::string COLON = ":";
const static std::string SEMICOLON = ";";
const static std::string DIVIDE = "/";
const static std::string EQUAL = "=";

constexpr int64_t READ_BATCH_SIZE = 8 * 1024 * 1024;  // 8K

/*!
 * \brief first is the indicator name, second is the description of the indicator
 */
const static std::pair<std::string, std::string> BINLOG_CPU_COUNT{"binlog_cpu_count", ""};
const static std::pair<std::string, std::string> BINLOG_CPU_USED_RATIO = {"binlog_cpu_used_ratio", ""};
const static std::pair<std::string, std::string> BINLOG_DISK_TOTAL_SIZE_MB = {"binlog_disk_total_size_mb", ""};
const static std::pair<std::string, std::string> BINLOG_DISK_USED_RATIO = {"binlog_disk_used_ratio", ""};
const static std::pair<std::string, std::string> BINLOG_DISK_USED_SIZE_MB = {"binlog_disk_used_size_mb", ""};
const static std::pair<std::string, std::string> BINLOG_MEM_TOTAL_SIZE_MB = {"binlog_mem_total_size_mb", ""};
const static std::pair<std::string, std::string> BINLOG_MEM_USED_RATIO = {"binlog_mem_used_ratio", ""};
const static std::pair<std::string, std::string> BINLOG_MEM_USED_SIZE_MB = {"binlog_mem_used_size_mb", ""};
const static std::pair<std::string, std::string> BINLOG_NETWORK_RX_BYTES = {"binlog_network_rx_bytes", ""};
const static std::pair<std::string, std::string> BINLOG_NETWORK_WX_BYTES = {"binlog_network_wx_bytes", ""};
const static std::pair<std::string, std::string> BINLOG_LOAD1 = {"binlog_load1", ""};
const static std::pair<std::string, std::string> BINLOG_LOAD5 = {"binlog_load5", ""};
const static std::pair<std::string, std::string> BINLOG_LOAD15 = {"binlog_load15", ""};

/*!
 * OBI related resource indicator information
 */
const static std::pair<std::string, std::string> BINLOG_INSTANCE_CPU_COUNT = {"binlog_instance_cpu_count", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_FD_COUNT = {"binlog_instance_fd_count", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_CPU_USED_RATIO = {
    "binlog_instance_cpu_used_ratio", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_DISK_TOTAL_SIZE_MB = {
    "binlog_instance_disk_total_size_mb", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_DISK_USED_RATIO = {
    "binlog_instance_disk_used_ratio", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_MEM_TOTAL_SIZE_MB = {
    "binlog_instance_mem_total_size_mb", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_MEM_USED_RATIO = {
    "binlog_instance_mem_used_ratio", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_MEM_USED_SIZE_MB = {
    "binlog_instance_mem_used_size_mb", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_NETWORK_RX_BYTES = {
    "binlog_instance_network_rx_bytes", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_NETWORK_WX_BYTES = {
    "binlog_instance_network_wx_bytes", ""};

/*!
 * OBCDC Resource Indicators
 */
const static std::pair<std::string, std::string> BINLOG_INSTANCE_OBCDC_MEM_USED_SIZE_MB = {
    "binlog_instance_obcdc_mem_used_size_mb", ""};

/*!
 * Log transformation metrics
 */
const static std::pair<std::string, std::string> BINLOG_INSTANCE_CONVERT_CHECKPOINT = {
    "binlog_instance_convert_checkpoint", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_CONVERT_DELAY = {"binlog_instance_convert_delay", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_CONVERT_FETCH_RPS = {
    "binlog_instance_convert_fetch_rps", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_CONVERT_IOPS = {"binlog_instance_convert_iops", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_CONVERT_STORAGE_RPS = {
    "binlog_instance_convert_storage_rps", ""};

/*!
 * OBCDC log conversion metric
 */
const static std::pair<std::string, std::string> BINLOG_INSTANCE_OBCDC_CHECKPOINT = {
    "binlog_instance_obcdc_checkpoint", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_OBCDC_DELAY = {"binlog_instance_obcdc_delay", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_OBCDC_RELEASE_RPS = {
    "binlog_instance_obcdc_release_rps", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_OBCDC_FETCH_CLOG_TRAFFIC = {
    "binlog_instance_obcdc_fetch_clog_traffic", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_OBCDC_NEED_SLOW_DOWN_COUNT = {
    "binlog_instance_obcdc_need_slow_down_count", ""};

/*!
 * viability metric
 */
const static std::pair<std::string, std::string> BINLOG_INSTANCE_OBCDC_DOWN = {"binlog_instance_obcdc_down", ""};

/*!
 * Subscription metrics
 */
const static std::pair<std::string, std::string> BINLOG_INSTANCE_DUMP_COUNT = {"binlog_instance_dump_count", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_DUMP_DELAY = {"binlog_instance_dump_delay", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_DUMP_EVENT_NUM = {
    "binlog_instance_dump_event_num", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_DUMP_EVENT_BYTES = {
    "binlog_instance_dump_event_bytes", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_DUMP_ERROR_COUNT = {
    "binlog_instance_dump_error_count", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_DUMP_RPS = {"binlog_instance_dump_rps", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_DUMP_HEARTBEAT_RPS = {
    "binlog_instance_dump_heartbeat_rps", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_DUMP_CHECKPOINT = {
    "binlog_instance_dump_checkpoint", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_DUMP_IOPS = {"binlog_instance_dump_iops", ""};

/*!
 * Load related indicator information
 */
const static std::pair<std::string, std::string> BINLOG_INSTANCE_MASTER_SWITCH_COUNT = {
    "binlog_instance_master_switch_count", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_REPLICATION_NUM = {
    "binlog_instance_replication_num", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_NUM = {"binlog_instance_num", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_DOWN = {"binlog_instance_down", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_CREATE = {"binlog_instance_create", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_RELEASE = {"binlog_instance_release", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_STOP = {"binlog_instance_stop", ""};
const static std::pair<std::string, std::string> BINLOG_CREATE = {"binlog_create", ""};
const static std::pair<std::string, std::string> BINLOG_RELEASE = {"binlog_release", ""};

/*!
 * Service request statistics
 */
const static std::pair<std::string, std::string> BINLOG_SQL_REQUEST_COUNT = {"binlog_sql_request_count", ""};
const static std::pair<std::string, std::string> BINLOG_DUMP_REQUEST_COUNT = {"binlog_dump_request_count", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_MASTER_SWITCH_FAILED_COUNT = {
    "binlog_instance_master_switch_failed_count", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_NO_MASTER_COUNT = {
    "binlog_instance_no_master_count", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_GTID_INCONSISTENT_COUNT = {
    "binlog_instance_gtid_inconsistent_count", ""};
const static std::pair<std::string, std::string> BINLOG_ALLOCATE_NODE_FAIL_COUNT = {
    "binlog_allocate_node_fail_count", ""};
const static std::pair<std::string, std::string> BINLOG_INSTANCE_FAILOVER_FAIL_COUNT = {
    "binlog_instance_failover_fail_count", ""};

const static std::pair<std::string, std::string> BINLOG_MANAGER_DOWN_COUNT = {"binlog_manager_down_count", ""};

/*!
 * \brief end
 */
const static std::pair<std::string, std::string> BINLOG_METRIC_END = {"binlog_metric_end", ""};


const static std::string OB_CLUSTER_ID = "ob_cluster_id";
const static std::string OB_TENANT_ID = "ob_tenant_id";


}  // namespace logproxy
}  // namespace oceanbase
