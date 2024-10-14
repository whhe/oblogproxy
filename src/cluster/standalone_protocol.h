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
#include <vector>
#include "cluster/node.h"
#include "task.h"
#include "cluster_protocol.h"
#include "fs_util.h"

namespace oceanbase::logproxy {
class StandAloneProtocol final : public ClusterProtocol {
public:
  explicit StandAloneProtocol(
      ClusterConfig* cluster_config, TaskExecutorCb& task_executor_cb, InstanceFailoverCb& recover_cb);

  ~StandAloneProtocol() override;

  int register_node() override;

  int unregister_node() override;

  int publish_task(Task& task) override;

  int drop_instances(std::vector<Task>& task_vec) override;

  int fetch_task(const Task& task, vector<Task*>& tasks) override;

  int fetch_unfinished_tasks(const string& node_id, vector<Task>& tasks) override;

  int fetch_recent_offline_task(const string& instance_name, Task& task) override;

  int query_all_nodes(vector<Node*>& nodes) override;

  int query_node_by_id(const std::string& node_id, Node& node) override;

  int query_nodes(vector<Node*>& nodes, const Node& condition) override;

  int query_nodes(vector<Node*>& nodes, std::set<State> node_states) override;

  int query_nodes(set<std::string>& node_ids, map<std::string, Node>& nodes) override;

  int query_instance_by_name(const std::string& instance_name, BinlogEntry& entry) override;

  int query_master_instance(const std::string& cluster, const std::string& tenant, std::string& instance_name) override;

  int determine_primary_instance(const std::string& cluster, const std::string& tenant,
      const std::string& instance_name, std::string& master) override;

  int reset_master_instance(
      const std::string& cluster, const std::string& tenant, const std::string& instance_name) override;

  int query_instances(std::vector<BinlogEntry*>& instances, std::string slot, SlotType type) override;

  int query_instances(vector<BinlogEntry*>& instances, const BinlogEntry& condition) override;

  int query_instances(vector<BinlogEntry*>& instances, const set<std::string>& instance_names) override;

  int query_instances(vector<BinlogEntry*>& instances, const set<InstanceState>& instance_states) override;

  int query_all_instances(vector<BinlogEntry*>& instances) override;

  int update_instances(const BinlogEntry& instance, const BinlogEntry& condition) override;

  int add_instance(const BinlogEntry& instance) override;

  int query_task_by_id(const std::string& task_id, Task& task) override;

  int update_task(Task& task) override;

  int update_tasks(Task& task, const Task& condition) override;

  int update_instance(const BinlogEntry& instance) override;

  int query_init_instance_config(ConfigTemplate& config, const std::string& key_name, const std::string& group,
      const std::string& cluster, const std::string& tenant, const std::string& instance) override;

  int query_instance_configs(std::vector<ConfigTemplate*>& configs, const std::string& group,
      const std::string& cluster, const std::string& tenant, const std::string& instance) override;

  int query_configs_by_granularity(
      std::map<std::string, std::string>& configs, Granularity granularity, const std::string& scope) override;

  int query_instance_configs(std::map<std::string, std::string>& configs, const std::string& group,
      const std::string& cluster, const std::string& tenant, const std::string& instance) override;

  int replace_config(std::string key, std::string value, Granularity granularity, const std::string& scope) override;

  int delete_config(std::string key, Granularity granularity, const std::string& scope) override;

  int boundary_gtid_seq(const std::string& cluster, const std::string& tenant,
      std::pair<InstanceGtidSeq, InstanceGtidSeq>& boundary_pair) override;

  int boundary_gtid_seq(
      const std::string& instance, std::pair<InstanceGtidSeq, InstanceGtidSeq>& boundary_pair) override;

  int find_gtid_seq_by_timestamp(
      const std::string& cluster, const std::string& tenant, uint64_t timestamp, InstanceGtidSeq& gtid_seq) override;

  int find_gtid_seq_by_timestamp(const std::string& instance, uint64_t timestamp, InstanceGtidSeq& gtid_seq) override;

  int remove_instance_gtid_seq(const std::string& instance) override;

  int remove_tenant_gtid_seq(const string& cluster, const string& tenant) override;

  int get_gtid_seq_checkpoint(vector<InstanceGtidSeq>& instance_gtid_seq_vec) override;

  int add_instance(const BinlogEntry& instance, Task& task) override;

  int query_tenant_surviving_instances(string& cluster, string& tenant, vector<BinlogEntry>& instances) override;

  int query_user_by_name(const std::string& name, User& user) override;

  int alter_user_password(const User& user, const std::string& new_password) override;

  int query_sys_user(User& user) override;

private:
  std::mutex _op_mutex;
  std::string _state_filename;
  LocalTaskManager _local_task_manager;
};
}  // namespace oceanbase::logproxy
