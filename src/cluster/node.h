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
#include "metric/sys_metric.h"
#include "codec/message.h"
#include "model.h"
#include "obcdc_config.h"
#include "instance_meta.h"

#define NODES_FILENAME "nodes"
#define NODE_LOCAL_FILENAME "node.local"
#define INSTANCES_FILENAME "instances"

using namespace oceanbase::binlog;

namespace oceanbase::logproxy {

enum State { ONLINE, SUSPECT, OFFLINE, PAUSED, GRAYSCALE, UNDEFINED = UINT32_MAX };

std::string node_state_print(uint16_t code);

struct SpecialConfig : public Object {
  MODEL_DCL(SpecialConfig);
  MODEL_DEF_STR(group_id, "");
  MODEL_DEF_STR(cluster_id, "");
  MODEL_DEF_STR(tenant_id, "");
  // Is it effective?
  MODEL_DEF_BOOL(status, false);

  explicit SpecialConfig(bool initialize)
  {}
};

MODEL_DEF(SpecialConfig);

struct NodeConfig : public Object {
  MODEL_DCL(NodeConfig);
  MODEL_DEF_STR(node_id, "");
  // Maximum number of connections,-1 means unlimited
  MODEL_DEF_INT(max_connections, -1);
  // Maximum number of instances (obi or logreader)
  MODEL_DEF_INT(max_instances, -1);
  // User level special configuration
  MODEL_DEF_BOOL(specific, false);
  MODEL_DEF_LIST(special_configs, SpecialConfig);
  /*!
   * @brief Determine the expiration time for node offline
   */
  MODEL_DEF_INT64(expiration, 10);

  explicit NodeConfig(bool initialize);
};

MODEL_DEF(NodeConfig);

struct Node : public Object {
  MODEL_DCL(Node);
  MODEL_DEF_STR(id, "");
  MODEL_DEF_STR(ip, "");
  MODEL_DEF_UINT32(port, 0);
  /*!
   * @brief State
   */
  MODEL_DEF_UINT32(state, State::UNDEFINED);
  MODEL_DEF_UINT32(incarnation, 0);
  MODEL_DEF_UINT64(last_modify, 0);
  MODEL_DEF_OBJECT(metric, SysMetric);
  MODEL_DEF_STR(region, "");
  MODEL_DEF_STR(zone, "");
  MODEL_DEF_STR(group, "");
  MODEL_DEF_OBJECT(node_config, NodeConfig);

public:
  explicit Node(bool initialize);

  std::string identifier() const
  {
    return id() + "(" + ip() + ":" + std::to_string(port()) + ")";
  }

  bool is_offline() const
  {
    return state() == State::OFFLINE;
  }
};
MODEL_DEF(Node);

struct InstanceGtidSeq : public Object {
  MODEL_DCL(InstanceGtidSeq);
  MODEL_DEF_STR(cluster, "");
  MODEL_DEF_STR(tenant, "");
  MODEL_DEF_STR(cluster_id, "");
  MODEL_DEF_STR(tenant_id, "");
  MODEL_DEF_STR(instance_name, "");
  MODEL_DEF_UINT64(commit_version_start, 0);
  MODEL_DEF_STR(xid_start, "");
  MODEL_DEF_UINT64(gtid_start, 0);
  MODEL_DEF_UINT64(trxs_num, 0);

  explicit InstanceGtidSeq(bool initialize);
};
MODEL_DEF(InstanceGtidSeq);

struct User : public Object {
  MODEL_DCL(User);
  MODEL_DEF_STR(username, "");
  MODEL_DEF_STR(password_sha1, "");
  MODEL_DEF_STR(password, "");
  MODEL_DEF_INT(tag, -1);

  explicit User(bool initialize);
};
MODEL_DEF(User);

struct BinlogEntry : public Object {
  MODEL_DCL(BinlogEntry);
  MODEL_DEF_STR(node_id, "");
  MODEL_DEF_STR(ip, "");
  MODEL_DEF_UINT32(port, 0);
  MODEL_DEF_STR(instance_name, "");
  MODEL_DEF_STR(cluster, "");
  MODEL_DEF_STR(tenant, "");
  MODEL_DEF_STR(cluster_id, "");
  MODEL_DEF_STR(tenant_id, "");
  MODEL_DEF_UINT32(pid, 0);
  MODEL_DEF_STR(work_path, "");
  MODEL_DEF_UINT16(state, InstanceState::UNDEFINED);
  MODEL_DEF_OBJECT(config, InstanceMeta);
  MODEL_DEF_UINT64(heartbeat, 0);
  MODEL_DEF_UINT64(delay, UINT64_MAX);
  MODEL_DEF_UINT64(min_dump_checkpoint, 0);

public:
  explicit BinlogEntry(bool initialize);

  bool equal(const BinlogEntry& entry);

  std::string full_tenant() const;

  void update_initial_gtid(const InstanceGtidSeq& gtid_seq);

  void update_initial_gtid(const BinlogEntry& entry);

  void init_instance(Node&, InstanceMeta*);

  bool is_dropped() const
  {
    return state() == InstanceState::DROP || state() == InstanceState::LOGICAL_DROP;
  }

  bool is_running() const
  {
    return InstanceState::GRAYSCALE == state() || InstanceState::RUNNING == state() ||
           InstanceState::STARTING == state() || InstanceState::INIT == state();
  }

  bool can_connected() const
  {
    return InstanceState::GRAYSCALE == state() || InstanceState::RUNNING == state();
  }
};

MODEL_DEF(BinlogEntry);
static std::function<std::string(BinlogEntry)> instance_name_gen = [](const BinlogEntry& entry) {
  return entry.instance_name();
};
static std::function<std::string(BinlogEntry*)> p_instance_name_gen = [](BinlogEntry* entry) {
  return entry->instance_name();
};

void deserialize_gtid_seq_str(
    const BinlogEntry& instance, const std::string& gtid_seq_str, std::vector<InstanceGtidSeq>& gtid_seq_vec);

enum Granularity { G_GLOBAL, G_GROUP, G_CLUSTER, G_TENANT, G_INSTANCE, G_UNDEFINED };

struct ConfigTemplate : public Object {
  MODEL_DCL(ConfigTemplate);
  MODEL_DEF_UINT64(id, 0);
  MODEL_DEF_STR(version, "");
  MODEL_DEF_STR(key_name, "");
  MODEL_DEF_STR(value, "");
  MODEL_DEF_UINT16(granularity, G_UNDEFINED);
  MODEL_DEF_STR(scope, "");

  explicit ConfigTemplate(bool initialize);
};

MODEL_DEF(ConfigTemplate);

struct PrimaryInstance : public Object {
  MODEL_DCL(PrimaryInstance);
  MODEL_DEF_STR(cluster, "");
  MODEL_DEF_STR(tenant, "");
  MODEL_DEF_STR(cluster_id, "");
  MODEL_DEF_STR(tenant_id, "");
  MODEL_DEF_STR(master_instance, "");

  explicit PrimaryInstance(bool initialize);
};
MODEL_DEF(PrimaryInstance);

struct Meta {
  //  std::vector<NodeState*> node_list;
};

}  // namespace oceanbase::logproxy
