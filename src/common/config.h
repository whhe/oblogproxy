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
#include <cstdint>

#include "common.h"
#include "config_base.h"

namespace oceanbase::logproxy {
enum LogType {
  /**
   * OceanBase LogReader
   */
  OCEANBASE = 0,
};

class Config : public ConfigBase {
public:
  Config() : ConfigBase(CONFIG){};

  static Config& instance()
  {
    static Config config_singleton;
    return config_singleton;
  }

public:
  virtual ~Config() = default;

  int load(const std::string& file);

  void init(std::map<std::string, std::string>& configs);

public:
  OMS_CONFIG_UINT16(service_port, 2983);
  OMS_CONFIG_UINT32(encode_threadpool_size, 8);
  OMS_CONFIG_UINT32(encode_queue_size, 20000);
  OMS_CONFIG_UINT32(max_packet_bytes, 1024 * 1024 * 64);  // 64MB
  OMS_CONFIG_UINT32(command_timeout_s, 10);
  OMS_CONFIG_UINT64(accept_interval_us, 500000);

  OMS_CONFIG_UINT32(record_queue_size, 20000);
  OMS_CONFIG_UINT64(read_timeout_us, 2000000);
  OMS_CONFIG_UINT64(read_fail_interval_us, 1000000);
  OMS_CONFIG_UINT32(read_wait_num, 20000);

  OMS_CONFIG_UINT64(send_timeout_us, 2000000);
  OMS_CONFIG_UINT64(send_fail_interval_us, 1000000);

  OMS_CONFIG_BOOL(check_quota_enable, false);
  /*!
   * @brief Whether to enable clog site verification
   */
  OMS_CONFIG_BOOL(check_clog_enable, true);

  OMS_CONFIG_BOOL(log_to_stdout, false);
  OMS_CONFIG_UINT32(log_quota_size_mb, 5120);
  OMS_CONFIG_UINT32(log_quota_day, 30);
  OMS_CONFIG_UINT32(log_gc_interval_s, 43200);
  OMS_CONFIG_UINT16(log_flush_strategy, 1);       // 0: log_flush_period_s, 1(default): log_flush_level
  OMS_CONFIG_UINT16(log_level, 2);                // default level: info
  OMS_CONFIG_UINT16(log_flush_level, 2);          // default level: warn
  OMS_CONFIG_UINT16(log_flush_period_s, 1);       // unit: second
  OMS_CONFIG_UINT16(log_max_file_size_mb, 1024);  // default: 1 GB
  OMS_CONFIG_UINT16(log_retention_h, 360);        // default: 15 Days

  OMS_CONFIG_STR(oblogreader_path, "./run");
  OMS_CONFIG_STR(bin_path, "./bin");
  OMS_CONFIG_UINT32(oblogreader_path_retain_hour, 168);  // 7 Days
  OMS_CONFIG_UINT32(oblogreader_lease_s, 300);           // 5 mins
  OMS_CONFIG_UINT32(oblogreader_max_count, 100);

  OMS_CONFIG_UINT32(max_cpu_ratio, 0);
  OMS_CONFIG_UINT64(max_mem_quota_mb, 0);

  OMS_CONFIG_BOOL(allow_all_tenant, false);
  OMS_CONFIG_BOOL(auth_user, true);
  OMS_CONFIG_BOOL(auth_use_rs, false);
  OMS_CONFIG_BOOL(auth_allow_sys_user, false);

  OMS_CONFIG_ENCRYPT(ob_sys_username, "");
  OMS_CONFIG_ENCRYPT(ob_sys_password, "");

  OMS_CONFIG_UINT64(ob_clog_fetch_interval_s, 600);  // 10 mins
  OMS_CONFIG_UINT64(ob_clog_expr_s, 43200);          // 12 h

  OMS_CONFIG_UINT32(counter_interval_s, 2);  // 2s
  OMS_CONFIG_BOOL(metric_enable, true);
  OMS_CONFIG_UINT32(metric_interval_s, 120);  // 2mins
  OMS_CONFIG_UINT16(prometheus_port, 2984);
  OMS_CONFIG_UINT16(prometheus_unused_metric_clear_interval_s, 30);

  // when builtin_cluster_url_prefix not empty, we read cluster_id in handshake to make an complete cluster_url
  OMS_CONFIG_STR(builtin_cluster_url_prefix, "");

  OMS_CONFIG_STR(communication_mode, "server");  // server mode or client mode

  // plain, tls refer ChannelFactory::init
  OMS_CONFIG_STR(channel_type, "plain");
  OMS_CONFIG_STR(tls_ca_cert_file, "");
  OMS_CONFIG_STR(tls_cert_file, "");
  OMS_CONFIG_STR(tls_key_file, "");
  OMS_CONFIG_BOOL(tls_verify_peer, true);

  // tls between observer and liboblog
  OMS_CONFIG_BOOL(liboblog_tls, true);
  OMS_CONFIG_STR(liboblog_tls_cert_path, "");

  // debug related
  OMS_CONFIG_BOOL(debug, true);            // enable debug mode
  OMS_CONFIG_BOOL(verbose, false);         // print more log
  OMS_CONFIG_BOOL(verbose_packet, false);  // print data packet info
  OMS_CONFIG_BOOL(readonly, false);        // only read from LogReader, use for test
  OMS_CONFIG_BOOL(count_record, false);
  OMS_CONFIG_BOOL(verbose_record_read, false);

  // for inner use
  OMS_CONFIG_UINT64(process_name_address, 0);
  OMS_CONFIG_BOOL(packet_magic, true);

  // for obcdc
  OMS_CONFIG_STR(oblogreader_obcdc_path_template, "../../obcdc/obcdc-%d.x-access/libobcdcaccess.so");
  OMS_CONFIG_STR(oblogreader_obcdc_ce_path_template, "../../obcdc/obcdc-ce-%d.x-access/libobcdcaccess.so");
  OMS_CONFIG_STR(binlog_obcdc_path_template, "../../obcdc/obcdc-%d.x-access/libobcdcaccess.so");
  OMS_CONFIG_STR(binlog_obcdc_ce_path_template, "../../obcdc/obcdc-ce-%d.x-access/libobcdcaccess.so");

  // for timezone_info.conf
  OMS_CONFIG_STR(oblogreader_timezone_conf, "../../conf/timezone_info.conf");
  OMS_CONFIG_STR(binlog_timezone_conf, "../../conf/timezone_info.conf");

  // for mysql binlog
  OMS_CONFIG_BOOL(binlog_ignore_unsupported_event, true);
  OMS_CONFIG_STR(binlog_log_bin_basename, "./run");
  OMS_CONFIG_STR(binlog_log_bin_prefix, "mysql-bin");
  OMS_CONFIG_UINT32(binlog_max_event_buffer_bytes, 1024 * 1024 * 64);  // 64MB
  OMS_CONFIG_BOOL(binlog_mode, false);
  OMS_CONFIG_BOOL(binlog_checksum, true);
  OMS_CONFIG_INT64(binlog_convert_timeout_us, 100000);
  OMS_CONFIG_UINT32(binlog_nof_work_threads, 16);
  OMS_CONFIG_UINT32(binlog_purge_binlog_threads, 2);
  OMS_CONFIG_UINT32(binlog_sql_work_threads, 4);
  OMS_CONFIG_UINT32(binlog_obi_column_work_threads, 10);
  OMS_CONFIG_UINT32(binlog_max_file_size_bytes, 1024 * 1024 * 512);  // 512MB
  OMS_CONFIG_UINT16(binlog_file_name_fill_zeroes_width, 6);          // fill zeroes width for mysql binlog file name
  OMS_CONFIG_UINT64(binlog_heartbeat_interval_us, 100000);           // The interval at which heartbeat events are sent
  OMS_CONFIG_UINT16(binlog_log_heartbeat_interval_times, 10);
  OMS_CONFIG_BOOL(binlog_ddl_convert, true);
  OMS_CONFIG_BOOL(binlog_ddl_convert_ignore_unsupported_ddl,
      true);  // Ignore unsupported DDL. If set to false, unsupported DDL will also be dropped into the binlog.
  OMS_CONFIG_STR(binlog_memory_limit, "4G");
  OMS_CONFIG_STR(binlog_working_mode, "storage");

  // for tcp port
  OMS_CONFIG_UINT16(start_tcp_port, 8100);
  OMS_CONFIG_UINT16(reserved_ports_num, 256);

  // Binlog incremental DDL conversion component etransfer path, which is the jar package introduced by JNI
  OMS_CONFIG_STR(binlog_ddl_convert_jvm_options, "-Djava.class.path=../../deps/lib/etransfer.jar");
  OMS_CONFIG_BOOL(binlog_gtid_display, true);  // Whether to display gtid information in show master status

  OMS_CONFIG_STR(binlog_ddl_convert_class, "com/alipay/oms/etransfer/util/OB2MySQLConvertTool");
  OMS_CONFIG_STR(binlog_ddl_convert_func, "parser");
  OMS_CONFIG_STR(binlog_ddl_convert_func_param, "(Ljava/lang/String;Ljava/lang/String;Z)Ljava/lang/String;");

  // node limit threshold
  /*!
   * When restoring binlog breakpoint, whether to enable backup?
   */
  OMS_CONFIG_BOOL(binlog_recover_backup, true);
  // for test
  OMS_CONFIG_STR(table_whitelist, "");
  OMS_CONFIG_UINT32(node_mem_limit_minimum_mb, 2048);
  OMS_CONFIG_UINT16(node_cpu_limit_threshold_percent, 80);
  OMS_CONFIG_UINT16(node_mem_limit_threshold_percent, 85);
  OMS_CONFIG_UINT16(node_disk_limit_threshold_percent, 70);

  // Definition of cluster related parameters
  OMS_CONFIG_STR(cluster_communication_address, "");
  OMS_CONFIG_UINT16(cluster_communication_port, 2984);
  OMS_CONFIG_UINT64(cluster_message_queue, 1000);
  OMS_CONFIG_STR(cluster_protocol, "database");
  OMS_CONFIG_BOOL(cluster_mode, true);

  OMS_CONFIG_STR(node_ip, "");
  OMS_CONFIG_STR(database_ip, "");
  OMS_CONFIG_UINT16(database_port, 2883);
  OMS_CONFIG_STR(database_name, "");
  OMS_CONFIG_STR(database_properties, "");
  OMS_CONFIG_STR(user, "");
  OMS_CONFIG_STR(password, "");
  OMS_CONFIG_INT32(min_pool_size, 30);
  OMS_CONFIG_UINT64(push_pull_interval_us, 1000000);
  OMS_CONFIG_UINT64(task_interval_us, 1000000);

  OMS_CONFIG_UINT64(gossip_interval_us, 10);
  OMS_CONFIG_UINT64(gossip_nodes, 2);

  // gtid seq
  OMS_CONFIG_UINT16(gtid_seq_compressed_interval_s, 10);    // default: 10s
  OMS_CONFIG_UINT32(gtid_seq_compressed_trx_size, 100000);  // default: 10w
  OMS_CONFIG_UINT32(gtid_marking_step_size, 100000);        // default: 10w
  OMS_CONFIG_UINT32(gtid_inspector_s, 900);
  OMS_CONFIG_UINT32(gtid_memory_cache_seconds, 7200);
  OMS_CONFIG_UINT32(gtid_heartbeat_duration_s, 3600);
  OMS_CONFIG_BOOL(enable_gtid_inspector, true);

  OMS_CONFIG_BOOL(enable_resource_check, true);
  OMS_CONFIG_STR(cpu_mem_disk_net_weighted, "1:1:1:1");
  OMS_CONFIG_STR(recovery_point_strategy, "fast");  // fast or complete

  // dumper resource check
  OMS_CONFIG_UINT32(max_dumper_num, 128);
  OMS_CONFIG_BOOL(enable_dumper_cpu_precheck, false);

  OMS_CONFIG_UINT16(default_instance_replicate_num, 1);
  OMS_CONFIG_UINT32(max_task_execution_time_s, 600);
  OMS_CONFIG_UINT32(default_defer_drop_sec, 7200);
  OMS_CONFIG_UINT16(max_instance_replicate_num, 5);
  OMS_CONFIG_UINT32(max_delete_rows, 10000);
  OMS_CONFIG_UINT32(max_instance_startup_wait_sec, 60);

  OMS_CONFIG_UINT32(enable_instance_proxy, false);
  OMS_CONFIG_UINT16(max_incr_gtid_seqs_num, 200);

  OMS_CONFIG_BOOL(enable_auth, false);
  // 2 to the Nth power
  OMS_CONFIG_INT32(binlog_convert_ring_buffer_size, 1024);
  OMS_CONFIG_INT32(binlog_convert_number_of_concurrences, 12);
  OMS_CONFIG_INT32(binlog_convert_thread_size, 16);

  // 2 to the Nth power
  OMS_CONFIG_INT32(binlog_serialize_ring_buffer_size, 1024);
  OMS_CONFIG_INT32(binlog_serialize_thread_size, 10);
  OMS_CONFIG_UINT32(binlog_serialize_parallel_size, 8);

  // 2 to the Nth power, binlog record release queue size
  OMS_CONFIG_INT32(binlog_release_ring_buffer_size, 1024);
  OMS_CONFIG_INT32(binlog_release_thread_size, 4);
  OMS_CONFIG_UINT32(binlog_release_parallel_size, 2);

  // pre_allocated_memory_for_each_event
  OMS_CONFIG_UINT32(preallocated_memory_bytes, 2 * 1024 * 1024);

  // When the pre-allocated memory is insufficient, the default expansion step size
  OMS_CONFIG_UINT32(preallocated_expansion_memory_bytes, 8 * 1024);
};

int load_configs(const std::string& config_file, rapidjson::Document& doc);
}  // namespace oceanbase::logproxy
