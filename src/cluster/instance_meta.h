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

#include "model.h"
#include "common/obcdc_config.h"

using namespace oceanbase::logproxy;

namespace oceanbase::binlog {

enum InstanceState { INIT, STARTING, RUNNING, FAILED, STOP, DROP, OFFLINE, PAUSED, GRAYSCALE, LOGICAL_DROP, UNDEFINED };

InstanceState value_of(uint8_t state);

std::string instance_state_str(uint8_t state);

enum SlotType { IP, ZONE, REGION, GROUP };

struct BinlogConfig : public Object {
  MODEL_DCL(BinlogConfig);
  MODEL_DEF_UINT32(master_server_id, 0);
  MODEL_DEF_STR(master_server_uuid, "");
  MODEL_DEF_UINT32(server_id, 0);
  MODEL_DEF_STR(server_uuid, "");

  // [0, 4294967295], default: 18h
  MODEL_DEF_UINT64(binlog_expire_logs_seconds, 64800);
  // [0, 9223372036854775807], default: 50GB
  MODEL_DEF_UINT64(binlog_expire_logs_size, 53687091200);
  // [4096, 1073741824], default: 512MB
  MODEL_DEF_UINT32(max_binlog_size, 536870912);

  // 1(ON/true)
  MODEL_DEF_UINT16(auto_start_obcdc, 1);
  // 0(OFF/false)
  MODEL_DEF_UINT16(active_state_after_boot, 0);
  // 1(ON/true)
  MODEL_DEF_UINT16(failover, 1);

  MODEL_DEF_STR(initial_ob_txn_id, "");
  MODEL_DEF_UINT64(initial_ob_txn_gtid_seq, 0);

  // throttle options
  MODEL_DEF_UINT64(throttle_convert_iops, 0);
  MODEL_DEF_UINT32(throttle_convert_rps, 0);
  MODEL_DEF_UINT64(throttle_dump_conn, 0);
  MODEL_DEF_UINT64(throttle_dump_iops, 0);
  MODEL_DEF_UINT32(throttle_dump_rps, 0);

  // used for login
  MODEL_DEF_STR(instance_user, "");
  MODEL_DEF_STR(instance_password, "");
  MODEL_DEF_STR(instance_password_sha1, "");

  // Concurrent convert options
  MODEL_DEF_UINT32(record_queue_size, 200000);
  MODEL_DEF_UINT32(read_wait_num, 20000);
  MODEL_DEF_UINT32(storage_wait_num, 100000);
  MODEL_DEF_UINT32(read_timeout_us, 10000);
  MODEL_DEF_UINT32(storage_timeout_us, 10000);
  // 2 to the Nth power
  MODEL_DEF_UINT32(binlog_convert_ring_buffer_size, 1024);
  MODEL_DEF_UINT32(binlog_convert_number_of_concurrences, 12);
  MODEL_DEF_UINT32(binlog_convert_thread_size, 16);

  // 2 to the Nth power
  MODEL_DEF_INT(binlog_serialize_ring_buffer_size, 8);
  MODEL_DEF_INT(binlog_serialize_thread_size, 10);
  MODEL_DEF_INT(binlog_serialize_parallel_size, 8);

  // 2 to the Nth power, binlog record release queue size
  MODEL_DEF_INT(binlog_release_ring_buffer_size, 1024);
  MODEL_DEF_INT(binlog_release_thread_size, 4);
  MODEL_DEF_INT(binlog_release_parallel_size, 2);

  // pre_allocated_memory_for_each_event
  MODEL_DEF_UINT64(preallocated_memory_bytes, 2 * 1024 * 1024);

  // When the pre-allocated memory is insufficient, the default expansion step size
  MODEL_DEF_UINT32(preallocated_expansion_memory_bytes, 8 * 1024);

  BinlogConfig(bool initialize)
  {}
};

MODEL_DEF(BinlogConfig);

struct SlotConfig : public Object {
  MODEL_DCL(SlotConfig);
  MODEL_DEF_STR(ip, "");
  MODEL_DEF_STR(zone, "");
  MODEL_DEF_STR(region, "");
  MODEL_DEF_STR(group, "");

  SlotConfig(bool initialize)
  {}
};

MODEL_DEF(SlotConfig);

struct CdcConfig : public Object {
  MODEL_DCL(CdcConfig);

  MODEL_DEF_UINT64(start_timestamp, 0);
  MODEL_DEF_STR(rootserver_list, "");
  MODEL_DEF_STR(cluster_url, "");
  MODEL_DEF_STR(cluster_user, "");
  MODEL_DEF_STR(cluster_password, "");
  MODEL_DEF_STR(extra_obcdc_cfg, "");

  MODEL_DEF_STR(memory_limit, "4G");

  CdcConfig(bool initialize)
  {}
};

MODEL_DEF(CdcConfig);

#define INSTANCE_CONFIG_FILE "binlog_instance.conf"
#define INSTANCE_PID_FILE "binlog_instance.pid"
#define INSTANCE_SOCKET_PATH "binlog_instance.socket"

class InstanceMeta : public Object {
  MODEL_DCL(InstanceMeta);

  MODEL_DEF_STR(instance_name, "");
  MODEL_DEF_STR(cluster, "");
  MODEL_DEF_STR(tenant, "");
  MODEL_DEF_STR(cluster_id, "");
  MODEL_DEF_STR(tenant_id, "");
  MODEL_DEF_STR(version, "");
  MODEL_DEF_INT(fd, -1);
  MODEL_DEF_INT64(send_interval_ms, 100);
  MODEL_DEF_OBJECT(binlog_config, BinlogConfig);
  MODEL_DEF_OBJECT(cdc_config, CdcConfig);
  MODEL_DEF_OBJECT(slot_config, SlotConfig);
  /*
   * The time to determine if the binlog instance is not alive (the time difference between the last reported heartbeat
   * and the current time), the default is 10s
   */
  MODEL_DEF_INT64(expiration, 10);

public:
  InstanceMeta(const Config& config, const std::string& local_cluster, const std::string& local_tenant);

  InstanceMeta(const Config& config, const std::string& local_instance_name, const std::string& local_cluster,
      const std::string& local_tenant);

  explicit InstanceMeta(bool initialize);

  static InstanceMeta& instance()
  {
    static InstanceMeta meta_singleton;
    meta_singleton.set_slot_config(new SlotConfig());
    meta_singleton.set_cdc_config(new CdcConfig());
    meta_singleton.set_binlog_config(new BinlogConfig());
    return meta_singleton;
  }

public:
  void init_meta(
      std::map<std::string, std::string>& instance_configs, std::map<std::string, std::string>& instance_options);

  int config_server_options();

  int config_primary_secondary_options();

  void update_instance_name(const std::string& l_instance_name);

  void update_version();

  ObcdcConfig& get_obcdc_config();

private:
  void init_obcdc_config(std::map<std::string, std::string>& instance_options, CdcConfig* cdc_config);

  void convert_to_obcdc_config(ObcdcConfig& obcdc_config);

private:
  Config _config;
  ObcdcConfig _obcdc_config;  // for inner use
};

MODEL_DEF(InstanceMeta);

}  // namespace oceanbase::binlog
