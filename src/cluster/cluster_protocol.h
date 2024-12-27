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
#include "node.h"
#include "task.h"
#include "cluster_config.h"
#include "functional"
#include "thread_pool_executor.h"
#include "instance_meta.h"

namespace oceanbase::logproxy {

using TaskExecutorCb = std::function<void(Task&)>;
using InstanceFailoverCb = std::function<void(const BinlogEntry&)>;

enum ProtocolMode { STANDALONE, DATABASE, GOSSIP };

class ClusterProtocol {
public:
  ClusterProtocol(ClusterConfig* cluster_config, TaskExecutorCb& task_executor_cb, InstanceFailoverCb& failover_cb);

  virtual ~ClusterProtocol() = 0;

  /*!
   * @brief Register the node into the cluster and communicate with the cluster
   * @return
   */
  virtual int register_node() = 0;

  virtual int unregister_node() = 0;

  /*!
   * @brief Publish tasks to the cluster
   * @param task
   * @return
   */
  virtual int publish_task(Task& task) = 0;

  virtual int add_instance(const BinlogEntry& instance, Task& task) = 0;

  virtual int drop_instances(std::vector<Task>& task_vec) = 0;

  virtual int query_task_by_id(const std::string& task_id, Task& task) = 0;

  /*!
   * @brief Get tasks from the cluster.
   * If node is not specified, that is, node is empty, all tasks will be pulled.
   * @param task
   * @param tasks
   * @return
   */
  virtual int fetch_task(const Task& task, std::vector<Task*>& tasks) = 0;

  virtual int fetch_unfinished_tasks(const std::string& node_id, std::vector<Task>& tasks) = 0;

  virtual int fetch_recent_offline_task(const std::string& instance_name, Task& task) = 0;

  virtual int update_task(Task& task) = 0;

  virtual int update_tasks(Task& task, const Task& condition) = 0;

  /*!
   * @brief Get information about all nodes in the cluster
   * @return
   */
  virtual int query_all_nodes(std::vector<Node*>& nodes) = 0;

  virtual int query_node_by_id(const std::string& node_id, Node& node) = 0;

  virtual int query_nodes(std::vector<Node*>& nodes, const Node& condition) = 0;

  virtual int query_nodes(std::vector<Node*>& nodes, std::set<State> node_states) = 0;

  virtual int query_nodes(std::set<std::string>& node_ids, std::map<std::string, Node>& nodes) = 0;
  /*!
   * @brief Query the corresponding instances according to the value specified by condition
   * @param instance_name
   * @param entry
   * @return
   */
  virtual int query_instance_by_name(const std::string& instance_name, BinlogEntry& entry) = 0;

  /*!
   * @brief
   * @param cluster cluster id
   * @param tenant tenant id
   * @param instance_name The master OBI name corresponding to the current tenant
   * @return Whether the action is successful
   */
  virtual int query_master_instance(
      const std::string& cluster, const std::string& tenant, std::string& instance_name) = 0;

  /*!
   * \brief
   * \param cluster cluster id
   * \param tenant tenant id
   * \param instance_name Instance of competition to choose the master
   * \param master After the master selection fails, return to the master that successfully selected
   * \return Whether the competition for the current instance is successful or not
   */
  virtual int determine_primary_instance(
      const std::string& cluster, const std::string& tenant, const std::string& instance_name, std::string& master) = 0;

  /*!
   * \brief
   * \param cluster cluster id
   * \param tenant tenant id
   * \param instance_name The name of the master who needs to go offline
   * \return
   */
  virtual int reset_master_instance(
      const std::string& cluster, const std::string& tenant, const std::string& instance_name) = 0;

  virtual int query_tenant_surviving_instances(
      std::string& cluster, std::string& tenant, std::vector<BinlogEntry>& instances) = 0;

  virtual int query_instances(std::vector<BinlogEntry*>& instances, const BinlogEntry& condition) = 0;

  virtual int query_all_instances(std::vector<BinlogEntry*>& instances) = 0;

  virtual int query_instances(std::vector<BinlogEntry*>& instances, const std::set<std::string>& instance_names) = 0;

  virtual int query_instances(std::vector<BinlogEntry*>& instances, const std::set<InstanceState>& instance_states) = 0;

  virtual int query_instances(std::vector<BinlogEntry*>& instances, std::string slot, SlotType type) = 0;

  virtual int update_instance(const BinlogEntry& instance) = 0;

  virtual int update_instances(const BinlogEntry& instance, const BinlogEntry& condition) = 0;

  virtual int add_instance(const BinlogEntry& instance) = 0;

  /*!
   * \brief
   * \param config Queryed configuration information
   * \param cluster The cluster corresponding to instance,When the instance has not yet been generated, the initial
   * configuration of the current instance should be judged based on its corresponding cluster, tenant, and group.
   * \param tenant The cluster tenant to instance
   * \param instance
   * \param key_name Configuration item key
   * \param group Logical group to which it belongs
   * \return
   */
  virtual int query_init_instance_config(ConfigTemplate& config, const std::string& key_name, const std::string& group,
      const std::string& cluster, const std::string& tenant, const std::string& instance) = 0;

  /*!
   * \brief
   * \param configs Configuration list
   * \param cluster The cluster corresponding to instance,When the instance has not yet been generated, the initial
   * configuration of the current instance should be judged based on its corresponding cluster, tenant, and group.
   * \param tenant
   * \param instance
   * \param group
   * \return
   */
  virtual int query_instance_configs(std::vector<ConfigTemplate*>& configs, const std::string& group,
      const std::string& cluster, const std::string& tenant, const std::string& instance) = 0;

  virtual int query_instance_configs(std::map<std::string, std::string>& configs, const std::string& group,
      const std::string& cluster, const std::string& tenant, const std::string& instance) = 0;

  virtual int query_configs_by_granularity(
      std::map<std::string, std::string>& configs, Granularity granularity, const std::string& scope) = 0;

  /*!
   * \brief  If the configuration item does not exist, insert it and update it if it exists.
   * \param key Configured keys
   * \param value configuration value
   * \param granularity granularity
   * \param scope The value corresponding to the granularity, such as G_CLUSTER granularity, the corresponding cluster
   * ID is xxxxx \return
   */
  virtual int replace_config(std::string key, std::string value, Granularity granularity, const std::string& scope) = 0;

  /*!
   * \brief  If the configuration exists, delete the specified configuration
   * \param key
   * \param granularity
   * \param scope
   * \return
   */
  virtual int delete_config(std::string key, Granularity granularity, const std::string& scope) = 0;

  virtual int boundary_gtid_seq(const std::string& cluster, const std::string& tenant,
      std::pair<InstanceGtidSeq, InstanceGtidSeq>& boundary_pair) = 0;

  virtual int boundary_gtid_seq(
      const std::string& instance, std::pair<InstanceGtidSeq, InstanceGtidSeq>& boundary_pair) = 0;

  virtual int find_gtid_seq_by_timestamp(
      const std::string& cluster, const std::string& tenant, uint64_t timestamp, InstanceGtidSeq& gtid_seq) = 0;

  virtual int find_gtid_seq_by_timestamp(
      const std::string& instance, uint64_t timestamp, InstanceGtidSeq& gtid_seq) = 0;

  virtual int remove_instance_gtid_seq(const std::string& instance) = 0;

  virtual int remove_tenant_gtid_seq(const std::string& cluster, const std::string& tenant) = 0;

  virtual int get_gtid_seq_checkpoint(std::vector<InstanceGtidSeq>& instance_gtid_seq_vec) = 0;

  virtual int query_user_by_name(const std::string& name, User& user) = 0;

  virtual int alter_user_password(const User& user, const std::string& new_password) = 0;

  virtual int query_sys_user(User& user) = 0;

  const Node& get_node_info();

  ClusterConfig* get_cluster_config();

  TaskExecutorCb& get_task_executor_cb();

  InstanceFailoverCb& get_instance_failover_cb();

  ThreadPoolExecutor& get_thread_executor();

protected:
  Node _node_info;
  ClusterConfig* _cluster_config;
  ScheduledPullTask _pull_task{this};
  TaskExecutorCb _task_executor_cb;
  InstanceFailoverCb _instance_failover_cb;
  ThreadPoolExecutor _thread_executor;
};

}  // namespace oceanbase::logproxy
