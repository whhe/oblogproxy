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

#include "database_protocol.h"

#include "timer.h"
#include "guard.hpp"
#include "binlog/common_util.h"

#include "binlog/password.h"

#include <csignal>
#include <binlog/env.h>
#include <metric/prometheus.h>

namespace oceanbase::logproxy {

int MetaDBProtocol::register_node()
{
  /*!
   * @brief Connect to the metadata database, register the node, update its heartbeat if the node already exists,
   * and pull all node information in the cluster to local persistence
   */
  Node node;
  node.set_id(this->get_node_info().id());
  auto conn = this->get_sql_connection().get_conn(true);
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  int ret = NodeDAO::query_node(conn, node);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to pull the node information [node_id:{},node_ip:{},node_port:{}] from the cluster",
        get_node_info().id(),
        get_node_info().ip(),
        get_node_info().port());
  }
  /*!
   * @brief The query result is empty, indicating that the node information does not exist in the cluster and needs to
   * be newly registered.
   */
  if (node.state() == UNDEFINED) {
    ret = NodeDAO::insert(conn, this->get_node_info());
    if (ret != OMS_OK) {
      OMS_ERROR("Failed to register node [node_id:{},node_ip:{},node_port:{}] to cluster",
          get_node_info().id(),
          get_node_info().ip(),
          get_node_info().port());
      return OMS_FAILED;
    }
  } else {
    ret = NodeDAO::update_node(conn, this->get_node_info());
    if (ret != OMS_OK) {
      OMS_ERROR("Failed to update node information [node_id:{},node_ip:{},node_port:{}] to cluster",
          get_node_info().id(),
          get_node_info().ip(),
          get_node_info().port());
      return OMS_FAILED;
    }
  }
  _push_pull_task.start();
  _metric_task.start();
  _pull_task.start();

  return OMS_OK;
}

int MetaDBProtocol::unregister_node()
{
  /*!
   * @brief Connect to the metadata database, register the node, update its heartbeat if the node already exists,
   * and pull all node information in the cluster to local persistence
   */
  std::vector<Node*> nodes;
  defer(release_vector(nodes));
  Node condition;
  condition.set_id(this->get_node_info().id());
  condition.set_ip(this->get_node_info().ip());
  condition.set_port(this->get_node_info().port());
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  if (NodeDAO::query_nodes(conn, nodes, condition) != OMS_OK) {
    OMS_ERROR("Failed to pull the node information [node_id:{},node_ip:{},node_port:{}] from the cluster",
        get_node_info().id(),
        get_node_info().ip(),
        get_node_info().port());
  }
  /*!
   * @brief The query result is empty, indicating that the node information does not exist in the cluster
   */
  if (nodes.empty()) {
    /*!
     * @brief do nothing
     */
    return OMS_OK;
  }

  if (NodeDAO::delete_nodes(conn, this->get_node_info()) != OMS_OK) {
    OMS_ERROR("Failed to delete node information [node_id:{},node_ip:{},node_port:{}] to cluster",
        get_node_info().id(),
        get_node_info().ip(),
        get_node_info().port());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int MetaDBProtocol::publish_task(Task& task)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));

  if (TaskDAO::add_task(conn, task) != OMS_OK) {
    OMS_ERROR("Failed to publish task to cluster,task:{}", task.serialize_to_json());
    return OMS_FAILED;
  }
  OMS_INFO("Successfully publish the task:{} to the cluster", task.serialize_to_json());
  return OMS_OK;
}

int MetaDBProtocol::add_instance(const BinlogEntry& instance, Task& task)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  conn->setAutoCommit(false);
  defer(conn->setAutoCommit(true));

  if (OMS_OK != BinlogInstanceDAO::add_instance(conn, instance)) {
    OMS_ERROR("Failed to add instance: {}", instance.serialize_to_json());
    conn->rollback();
    return OMS_FAILED;
  }
  if (OMS_OK != TaskDAO::add_task(conn, task)) {
    OMS_ERROR("Failed to publish task [{}] for adding instance, try to rollback all", task.serialize_to_json());
    conn->rollback();
    return OMS_FAILED;
  }

  conn->commit();
  OMS_INFO("Successfully publish [add instance] task: {}", task.serialize_to_json());
  return OMS_OK;
}

int MetaDBProtocol::drop_instances(std::vector<Task>& task_vec)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  conn->setAutoCommit(false);
  defer(conn->setAutoCommit(true));

  for (auto& task : task_vec) {
    if (BinlogInstanceDAO::update_instance_state(conn, task.instance_name(), InstanceState::LOGICAL_DROP)) {
      OMS_ERROR("Failed to update instance [{}] state to [LogicalDrop], try to rollback all", task.instance_name());
      conn->rollback();
      return OMS_FAILED;
    }

    if (TaskDAO::add_task(conn, task) != OMS_OK) {
      OMS_ERROR("Failed to publish task [{}] to cluster, try to rollback all", task.serialize_to_json());
      conn->rollback();
      return OMS_FAILED;
    }
  }

  conn->commit();
  OMS_INFO("Successfully publish the [{}] tasks to the cluster", task_vec.size());
  return OMS_OK;
}

int MetaDBProtocol::fetch_task(const Task& task, std::vector<Task*>& tasks)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));

  if (TaskDAO::query_tasks_by_entry(conn, tasks, task) != OMS_OK) {
    OMS_ERROR("Failed to fetch task from cluster,task:{}", task.serialize_to_json());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int MetaDBProtocol::fetch_unfinished_tasks(const string& node_id, std::vector<Task>& tasks)
{
  std::string query_sql = "SELECT * FROM tasks WHERE execution_node = \"" + node_id + "\" AND status < " +
                          std::to_string(TaskStatus::COMPLETED) +
                          " OR (status = " + std::to_string(TaskStatus::FAILED) + " AND retry_count < " +
                          std::to_string(MAX_TASK_RETRY_TIMES) + ")";
  return execute_multi_rows_query(get_sql_connection(), query_sql, tasks, task_rs_converter);
}

int MetaDBProtocol::fetch_recent_offline_task(const string& instance_name, Task& task)
{
  uint8_t offset = MAX_TASK_RETRY_TIMES - 1;
  std::string query_sql = "SELECT * FROM tasks WHERE type = " + std::to_string(TaskType::RECOVER_INSTANCE) +
                          " AND instance_name =\"" + instance_name + "\" ORDER BY last_modify DESC LIMIT " +
                          std::to_string(offset) + ", 1";
  return execute_at_most_one_query(get_sql_connection(), query_sql, task, task_rs_converter);
}

int MetaDBProtocol::query_all_nodes(std::vector<Node*>& nodes)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));

  if (NodeDAO::query_all_nodes(conn, nodes) != OMS_OK) {
    OMS_ERROR("Failed to query all nodes from cluster");
    return OMS_FAILED;
  }
  return OMS_OK;
}

int MetaDBProtocol::query_nodes(vector<Node*>& nodes, const Node& node)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));

  if (NodeDAO::query_nodes(conn, nodes, node) != OMS_OK) {
    OMS_ERROR("Failed to query nodes from cluster");
    return OMS_FAILED;
  }
  return OMS_OK;
}

MetaDBProtocol::~MetaDBProtocol()
{
  _push_pull_task.stop();
  _push_pull_task.join();
  _metric_task.stop();
  _metric_task.join();
  _pull_task.stop();
  _pull_task.join();
}

MetaDBProtocol::MetaDBProtocol(
    ClusterConfig* cluster_config, TaskExecutorCb& task_executor_cb, InstanceFailoverCb& failover_cb)
    : ClusterProtocol(cluster_config, task_executor_cb, failover_cb)
{
  sql::Properties properties(CommonUtils::serialized_kv_config(cluster_config->database_properties()));
  if (_sql_connection.init(cluster_config->database_ip(),
          cluster_config->database_port(),
          cluster_config->user(),
          cluster_config->password(),
          cluster_config->database_name(),
          properties,
          cluster_config->min_pool_size()) != OMS_OK) {
    OMS_ERROR("Failed to initialize metabase connection,ip:{},port:{},user:{},database:{},properties:{}",
        cluster_config->database_ip(),
        cluster_config->database_port(),
        cluster_config->password(),
        cluster_config->database_name(),
        cluster_config->database_properties());
    return;
  }
}

MySQLConnection& MetaDBProtocol::get_sql_connection()
{
  return _sql_connection;
}

int MetaDBProtocol::query_instances(std::vector<BinlogEntry*>& instances, const BinlogEntry& condition)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  return BinlogInstanceDAO::query_instance_by_entry(conn, instances, condition);
}

int MetaDBProtocol::query_nodes(vector<Node*>& nodes, const std::set<State> node_states)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  return NodeDAO::query_nodes(conn, nodes, node_states);
}

int MetaDBProtocol::query_nodes(set<std::string>& node_ids, map<std::string, Node>& nodes)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  if (node_ids.empty()) {
    OMS_ERROR("Empty node ids");
    return OMS_FAILED;
  }
  return NodeDAO::query_nodes(conn, node_ids, nodes);
}

int MetaDBProtocol::query_instances(vector<BinlogEntry*>& instances, const set<std::string>& instance_names)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  return BinlogInstanceDAO::query_instances(conn, instances, instance_names);
}

int MetaDBProtocol::query_instances(vector<BinlogEntry*>& instances, const set<InstanceState>& instance_states)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  return BinlogInstanceDAO::query_instances(conn, instances, instance_states);
}

int MetaDBProtocol::update_instances(const BinlogEntry& instance, const BinlogEntry& condition)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  return BinlogInstanceDAO::update_instance(conn, instance, condition);
}

int MetaDBProtocol::add_instance(const BinlogEntry& instance)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  return BinlogInstanceDAO::add_instance(conn, instance);
}

int MetaDBProtocol::update_task(Task& task)
{
  Task condition;
  condition.set_task_id(task.task_id());
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  return TaskDAO::update_task(conn, task, condition);
}

int MetaDBProtocol::update_tasks(Task& task, const Task& condition)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  return TaskDAO::update_task(conn, task, condition);
}

int MetaDBProtocol::update_instance(const BinlogEntry& instance)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  BinlogEntry condition;
  condition.set_instance_name(instance.instance_name());
  return BinlogInstanceDAO::update_instance(conn, instance, condition);
}

void MetaDBProtocol::query_config_item_by_granularity(
    ConfigTemplate& config, sql::Connection* conn, const ConfigTemplate& condition)
{
  auto* temp = new ConfigTemplate(true);
  defer(delete temp);
  ConfigTemplateDAO::query_config_item(conn, *temp, condition);
  if (!temp->key_name().empty()) {
    config.assign(temp);
  }
}

int MetaDBProtocol::query_init_instance_config(ConfigTemplate& config, const std::string& key_name,
    const std::string& group, const std::string& cluster, const std::string& tenant, const std::string& instance)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  ConfigTemplate condition;
  condition.set_key_name(key_name);
  /*
   * Query the global configuration first, and then query the configuration level by level. If it exists, the global
   * configuration will be overwritten. If it does not exist, fill in the configuration
   */
  {
    condition.set_granularity(G_GLOBAL);
    ConfigTemplateDAO::query_config_item(conn, config, condition);
  }

  if (!group.empty()) {
    condition.set_granularity(G_GROUP);
    condition.set_scope(group);
    query_config_item_by_granularity(config, conn, condition);
  }

  if (!cluster.empty()) {
    condition.set_granularity(G_CLUSTER);
    condition.set_scope(cluster);
    query_config_item_by_granularity(config, conn, condition);
  }

  if (!tenant.empty()) {
    condition.set_granularity(G_TENANT);
    condition.set_scope(tenant);
    query_config_item_by_granularity(config, conn, condition);
  }

  if (!instance.empty()) {
    condition.set_granularity(G_INSTANCE);
    condition.set_scope(instance);
    query_config_item_by_granularity(config, conn, condition);
  }

  return OMS_OK;
}

int MetaDBProtocol::query_instance_configs(std::vector<ConfigTemplate*>& configs, const std::string& group,
    const std::string& cluster, const std::string& tenant, const std::string& instance)
{
  return OMS_OK;
}

int MetaDBProtocol::query_configs_by_granularity(
    std::map<std::string, std::string>& configs, Granularity granularity, const std::string& scope)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  ConfigTemplate condition;
  condition.set_scope(scope);
  condition.set_granularity(granularity);
  return ConfigTemplateDAO::query_config_items(conn, configs, condition);
}

int MetaDBProtocol::query_instance_configs(std::map<std::string, std::string>& configs, const std::string& group,
    const std::string& cluster, const std::string& tenant, const std::string& instance)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  ConfigTemplate condition;
  /*
   * Query the global configuration first, and then query the configuration level by level. If it exists, the global
   * configuration will be overwritten. If it does not exist, fill in the configuration
   */
  {
    condition.set_granularity(G_GLOBAL);
    ConfigTemplateDAO::query_config_items(conn, configs, condition);
  }

  if (!group.empty()) {
    condition.set_granularity(G_GROUP);
    condition.set_scope(group);
    ConfigTemplateDAO::query_config_items(conn, configs, condition);
  }

  if (!cluster.empty()) {
    condition.set_granularity(G_CLUSTER);
    condition.set_scope(cluster);
    ConfigTemplateDAO::query_config_items(conn, configs, condition);
  }

  if (!tenant.empty()) {
    condition.set_granularity(G_TENANT);
    condition.set_scope(tenant);
    ConfigTemplateDAO::query_config_items(conn, configs, condition);
  }

  if (!instance.empty()) {
    condition.set_granularity(G_INSTANCE);
    condition.set_scope(instance);
    ConfigTemplateDAO::query_config_items(conn, configs, condition);
  }

  return OMS_OK;
}

int MetaDBProtocol::replace_config(
    std::string key, std::string value, Granularity granularity, const std::string& scope)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  ConfigTemplate entry;
  entry.set_key_name(key);
  entry.set_value(value);
  entry.set_granularity(granularity);
  entry.set_scope(scope);
  return ConfigTemplateDAO::replace_into_config_item(conn, entry);
}

int MetaDBProtocol::delete_config(std::string key, Granularity granularity, const std::string& scope)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  ConfigTemplate entry;
  entry.set_key_name(key);
  entry.set_granularity(granularity);
  entry.set_scope(scope);
  return ConfigTemplateDAO::delete_config_item(conn, entry);
}

int MetaDBProtocol::query_all_instances(vector<BinlogEntry*>& instances)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  return BinlogInstanceDAO::query_all_instances(conn, instances);
}

int MetaDBProtocol::query_node_by_id(const string& node_id, Node& node)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  node.set_id(node_id);
  return NodeDAO::query_node(conn, node);
}

int MetaDBProtocol::query_task_by_id(const string& task_id, Task& task)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  Task condition;
  condition.set_task_id(task_id);
  return TaskDAO::query_task_by_entry(conn, task, condition);
}

int MetaDBProtocol::query_instance_by_name(const string& instance_name, BinlogEntry& entry)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  return BinlogInstanceDAO::query_instance(conn, entry, instance_name);
}

int MetaDBProtocol::query_master_instance(
    const std::string& cluster, const std::string& tenant, std::string& instance_name)
{
  PrimaryInstance primary_instance;
  PrimaryInstance condition;
  auto conn = this->get_sql_connection().get_conn(true);
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  condition.set_cluster(cluster);
  condition.set_tenant(tenant);
  int ret = PrimaryInstanceDAO::query_primary_instance(conn, primary_instance, condition);
  if (ret != OMS_OK || primary_instance.master_instance().empty()) {
    return ret;
  }
  instance_name = primary_instance.master_instance();
  OMS_DEBUG("Query instance: {}", instance_name);
  return OMS_OK;
}

int MetaDBProtocol::determine_primary_instance(
    const std::string& cluster, const std::string& tenant, const std::string& instance_name, std::string& master)
{
  PrimaryInstance primary_instance;
  PrimaryInstance condition;
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  condition.set_cluster(cluster);
  condition.set_tenant(tenant);
  int ret = PrimaryInstanceDAO::query_primary_instance(conn, primary_instance, condition);

  if (ret != OMS_OK) {
    OMS_ERROR(
        "[determine primary] Failed to query the primary instance of the current tenant:[{},{}]", cluster, tenant);
    return OMS_FAILED;
  }

  PrimaryInstance entry;
  entry.set_cluster(cluster);
  entry.set_tenant(tenant);
  entry.set_master_instance(instance_name);
  /*
   * An empty result set means that there is no relevant information about the tenant in the current primary selection
   * table, so a record needs to be inserted.
   */
  if (primary_instance.cluster().empty() || primary_instance.tenant().empty()) {
    ret = PrimaryInstanceDAO::add_primary_instance(conn, entry);
    if (ret != OMS_OK) {
      OMS_ERROR(
          "[determine primary] Failed to add the primary instance of the current tenant:[{},{}]", cluster, tenant);
      return OMS_FAILED;
    }
    master = instance_name;
    OMS_INFO(
        "[determine primary] Succeed to determine the primary instance of the current tenant:[{},{}]", cluster, tenant);
  } else {
    /*
     *Optimistic lock update master selection
     */
    ret = PrimaryInstanceDAO::update_primary_instance(conn, entry, condition);
    if (ret != OMS_OK) {
      OMS_ERROR("[determine primary] Failed to determine the primary instance of the current tenant:[{},{}]",
          cluster,
          tenant);
      PrimaryInstanceDAO::query_primary_instance(conn, primary_instance, condition);
      master = primary_instance.master_instance();
      return OMS_OK;
    }
    master = instance_name;
    OMS_INFO(
        "[determine primary] Succeed to determine the primary instance of the current tenant:[{},{}]", cluster, tenant);
  }

  return OMS_OK;
}

int MetaDBProtocol::reset_master_instance(
    const std::string& cluster, const std::string& tenant, const std::string& instance_name)
{
  PrimaryInstance primary_instance;
  PrimaryInstance condition;
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  condition.set_cluster(cluster);
  condition.set_tenant(tenant);
  condition.set_master_instance(instance_name);

  primary_instance.set_cluster(cluster);
  primary_instance.set_tenant(tenant);
  // Set master to empty
  primary_instance.set_master_instance("");

  int ret = PrimaryInstanceDAO::update_primary_instance(conn, primary_instance, condition);
  if (ret != OMS_OK) {
    OMS_ERROR("[reset primary] Failed to reset the primary instance of the current tenant:[{},{}]", cluster, tenant);
    return OMS_FAILED;
  }
  OMS_INFO("[reset primary] Succeed to reset the primary instance of the current tenant:[{},{}]", cluster, tenant);
  return OMS_OK;
}

int MetaDBProtocol::query_tenant_surviving_instances(string& cluster, string& tenant, vector<BinlogEntry>& instances)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));

  return BinlogInstanceDAO::query_tenant_surviving_instances(conn, cluster, tenant, instances);
}

int MetaDBProtocol::query_instances(vector<BinlogEntry*>& instances, std::string slot, SlotType type)
{
  Node node;
  switch (type) {
    case IP:
      node.set_ip(slot);
      break;
    case ZONE:
      node.set_zone(slot);
      break;
    case REGION:
      node.set_region(slot);
      break;
    case GROUP:
      node.set_group(slot);
      break;
  }

  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));

  return BinlogInstanceDAO::query_instances_by_node(conn, instances, node);
}

int MetaDBProtocol::boundary_gtid_seq(
    const string& cluster, const string& tenant, pair<InstanceGtidSeq, InstanceGtidSeq>& boundary_pair)
{
  std::string first_gtid_query_sql = "SELECT * FROM instances_gtid_seq WHERE cluster = \"" + cluster +
                                     "\" AND tenant = \"" + tenant + "\" ORDER BY commit_version_start ASC LIMIT 1";
  if (OMS_OK != execute_at_most_one_query(
                    get_sql_connection(), first_gtid_query_sql, boundary_pair.first, gtid_seq_rs_converter)) {
    OMS_ERROR("Failed to query the first gtid of tenant: {}.{}", cluster, tenant);
    return OMS_FAILED;
  }

  std::string last_gtid_query_sql = "SELECT * FROM instances_gtid_seq WHERE cluster = \"" + cluster +
                                    "\" AND tenant = \"" + tenant + "\" ORDER BY commit_version_start DESC LIMIT 1";
  if (OMS_OK != execute_at_most_one_query(
                    get_sql_connection(), last_gtid_query_sql, boundary_pair.second, gtid_seq_rs_converter)) {
    OMS_ERROR("Failed to query the last gtid of tenant: {}.{}", cluster, tenant);
    return OMS_FAILED;
  }

  OMS_INFO("Tenant [{}.{}] boundary gtid seq: [{}, {}]",
      cluster,
      tenant,
      boundary_pair.first.serialize_to_json(),
      boundary_pair.second.serialize_to_json());
  return OMS_OK;
}

int MetaDBProtocol::boundary_gtid_seq(const string& instance, pair<InstanceGtidSeq, InstanceGtidSeq>& boundary_pair)
{
  auto conn = this->get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection.");
    return OMS_FAILED;
  }
  defer(this->get_sql_connection().release_connection(conn));
  return InstanceGtidSeqDAO::get_instance_boundary_gtid_seq(conn, instance, boundary_pair);
}

int MetaDBProtocol::find_gtid_seq_by_timestamp(
    const string& cluster, const string& tenant, uint64_t timestamp, InstanceGtidSeq& gtid_seq)
{
  std::string query_sql = "SELECT * FROM instances_gtid_seq WHERE cluster =\"" + cluster + "\" AND tenant=\"" + tenant +
                          "\" AND commit_version_start <= " + std::to_string(timestamp) +
                          " ORDER BY commit_version_start DESC LIMIT 1";
  OMS_DEBUG("Find gtid seq sql: {}", query_sql);
  return execute_at_most_one_query(get_sql_connection(), query_sql, gtid_seq, gtid_seq_rs_converter);
}

int MetaDBProtocol::find_gtid_seq_by_timestamp(const string& instance, uint64_t timestamp, InstanceGtidSeq& gtid_seq)
{
  std::string query_sql = "SELECT * FROM instances_gtid_seq WHERE instance =\"" + instance +
                          "\" AND commit_version_start <= " + std::to_string(timestamp) +
                          " ORDER BY commit_version_start DESC LIMIT 1";
  OMS_DEBUG("Find gtid seq sql: {}", query_sql);
  return execute_at_most_one_query(get_sql_connection(), query_sql, gtid_seq, gtid_seq_rs_converter);
}

int MetaDBProtocol::remove_instance_gtid_seq(const std::string& instance)
{
  OMS_INFO("Remove all gtid seqs of instance [{}]", instance);
  std::string remove_instance_gtid_seq = "DELETE FROM instances_gtid_seq WHERE instance = \"" + instance + "\" LIMIT " +
                                         std::to_string(Config::instance().max_delete_rows.val());

  int32_t affected_rows = 0;
  do {
    if (OMS_OK != execute_update(get_sql_connection(), remove_instance_gtid_seq, affected_rows)) {
      return OMS_FAILED;
    }
    OMS_DEBUG("Removed {} rows of instance {}", affected_rows, instance);
  } while (affected_rows > 0);
  return OMS_OK;
}

int MetaDBProtocol::remove_tenant_gtid_seq(const string& cluster, const string& tenant)
{
  OMS_INFO("Remove all gtid seqs of tenant [{}.{}]", cluster, tenant);
  std::string remove_instance_gtid_seq = "DELETE FROM instances_gtid_seq WHERE cluster = \"" + cluster +
                                         "\" AND tenant = \"" + tenant + "\" LIMIT " +
                                         std::to_string(Config::instance().max_delete_rows.val());

  int32_t affected_rows = 0;
  do {
    if (OMS_OK != execute_update(get_sql_connection(), remove_instance_gtid_seq, affected_rows)) {
      return OMS_FAILED;
    }
    OMS_DEBUG("Removed {} rows of tenant {}.{}", affected_rows, cluster, tenant);
  } while (affected_rows > 0);
  return OMS_OK;
}

int MetaDBProtocol::get_gtid_seq_checkpoint(std::vector<InstanceGtidSeq>& instance_gtid_seq_vec)
{
  std::string query = "SELECT * FROM instances_gtid_seq WHERE gtid_start > 0 AND gtid_start % " +
                      std::to_string(Config::instance().gtid_marking_step_size.val()) + " = 0 AND xid_start != ''";
  return execute_multi_rows_query(get_sql_connection(), query, instance_gtid_seq_vec, gtid_seq_rs_converter);
}

int MetaDBProtocol::query_user_by_name(const string& name, User& user)
{
  std::string query_sql = "SELECT * FROM user WHERE username = \"" + name + "\"";
  return execute_at_most_one_query(get_sql_connection(), query_sql, user, user_rs_converter);
}

int MetaDBProtocol::alter_user_password(const User& user, const string& new_password)
{
  std::string password_stage1_hex;
  std::string password_stage2_hex;
  if (!new_password.empty()) {
    char password_stage1[SHA1::SHA1_HASH_SIZE];
    char password_stage2[SHA1::SHA1_HASH_SIZE];
    if (OMS_OK !=
        compute_two_stage_sha1_hash(new_password.c_str(), new_password.size(), password_stage1, password_stage2)) {
      OMS_ERROR("Compute two stage sha1 hash for password [{}] failed", new_password);
      return OMS_FAILED;
    }
    dumphex(password_stage1, SHA1::SHA1_HASH_SIZE, password_stage1_hex);
    dumphex(password_stage2, SHA1::SHA1_HASH_SIZE, password_stage2_hex);
  }

  std::string set_password_sql = "UPDATE user SET password = \"" + password_stage2_hex + "\", password_sha1 = \"" +
                                 password_stage1_hex + "\" WHERE username = \"" + user.username() +
                                 "\" AND password = \"" + user.password() + "\" AND password_sha1 = \"" +
                                 user.password_sha1() + "\"";
  int32_t affected_rows = 0;
  int res = execute_update(get_sql_connection(), set_password_sql, affected_rows);
  if (res == OMS_OK && affected_rows == 0) {
    OMS_ERROR("Failed to update password for user [{}] with password [{}] to [{}], affected rows = 0",
        user.username(),
        user.password(),
        new_password);
    return OMS_FAILED;
  }
  return res;
}

int MetaDBProtocol::query_sys_user(User& user)
{
  std::string query_sql = "SELECT * FROM user WHERE tag = 0";
  return execute_at_most_one_query(get_sql_connection(), query_sql, user, user_rs_converter);
}

int NodeDAO::query_all_nodes(sql::Connection* conn, vector<Node*>& nodes)
{
  try {
    std::unique_ptr<sql::Statement> statement(conn->createStatement());
    sql::ResultSet* res = statement->executeQuery("SELECT * FROM nodes");
    defer(delete res);
    // Loop through and print results
    while (res->next()) {
      Node* node = new Node();
      convert_to_node(res, *node);
      nodes.push_back(node);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Error selecting tasks: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int NodeDAO::query_nodes(sql::Connection* conn, std::vector<Node*>& nodes, const Node& node)
{
  try {
    std::string sql = "SELECT * FROM nodes";
    sql += nodes_where_clause_sql(node);
    std::shared_ptr<sql::PreparedStatement> statement(conn->prepareStatement(sql));
    int count = 1;
    assgin_nodes(node, statement, count);
    sql::ResultSet* res = statement->executeQuery();
    defer(delete res);
    while (res->next()) {
      Node* ret_node = new Node();
      convert_to_node(res, *ret_node);
      OMS_DEBUG("query node info:{}", ret_node->serialize_to_json());
      nodes.push_back(ret_node);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to selecting nodes: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int NodeDAO::query_metric(sql::Connection* conn, SysMetric& metric, const Node& node)
{
  try {
    std::shared_ptr<sql::PreparedStatement> statement(
        conn->prepareStatement("SELECT metric FROM nodes WHERE id = ? AND ip = ? AND port = ? LIMIT 1"));
    statement->setString(1, node.id());
    statement->setString(2, node.ip());
    statement->setUInt(3, node.port());

    sql::ResultSet* res = statement->executeQuery();
    defer(delete res);
    res->next();
    Node* ret_node = new Node();
    defer(delete ret_node);
    convert_to_node(res, *ret_node);
    metric.assign(ret_node->metric());
  } catch (sql::SQLException& e) {
    OMS_ERROR("Error selecting tasks: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int NodeDAO::insert(sql::Connection* conn, const Node& node)
{
  try {
    std::string sql = add_node_sql(node);
    std::shared_ptr<sql::PreparedStatement> statement(conn->prepareStatement(sql));
    int count = 1;
    assgin_nodes(node, statement, count);
    int ret = statement->executeUpdate();
    OMS_DEBUG("Data was successfully inserted, the number of rows affected is: {}", ret);
  } catch (sql::SQLException& e) {
    OMS_ERROR("Error add node [{},{},{}] : {}", node.id(), node.ip(), node.port(), e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int NodeDAO::update_node(sql::Connection* conn, const Node& node, const Node& condition)
{
  try {
    std::string sql = "UPDATE nodes SET ";
    nodes_set_clause(node, sql);
    std::string where_clause = nodes_where_clause_sql(condition);
    sql += where_clause;
    OMS_DEBUG("Execute SQL:{}", sql);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assgin_nodes(node, stmt, count);
    assgin_nodes(condition, stmt, count);

    int ret = stmt->executeUpdate();
    OMS_DEBUG("Data was successfully updated, the number of rows affected is: {}", ret);
  } catch (sql::SQLException& e) {
    OMS_ERROR("Error update node [{},{},{}] : {}", node.id(), node.ip(), node.port(), e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

void NodeDAO::nodes_set_clause(const Node& node, string& sql)
{
  int count = 0;

  if (!node.id().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "id = ?";
    count++;
  }

  if (!node.ip().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "ip = ?";
    count++;
  }

  if (node.port() != 0) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "port = ?";
    count++;
  }

  if (node.state() != UNDEFINED) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "state = ?";
    count++;
  }

  if (node.metric() != nullptr) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "metric = ?";
    count++;
  }

  if (node.node_config() != nullptr) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "node_config = ?";
    count++;
  }

  if (!node.region().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "region = ?";
    count++;
  }

  if (!node.zone().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "zone = ?";
    count++;
  }

  if (node.incarnation() != 0) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "incarnation = ?";
    count++;
  }

  if (node.last_modify() != 0) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "last_modify = ?";
    count++;
  }

  if (!node.group().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "`group` = ?";
    count++;
  }
}

int NodeDAO::delete_nodes(sql::Connection* conn, const Node& node)
{
  try {
    std::string sql = "DELETE FROM nodes ";
    string where_clause = nodes_where_clause_sql(node);
    sql += where_clause;
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assgin_nodes(node, stmt, count);
    int ret = stmt->executeUpdate();
    OMS_DEBUG("Data was successfully deleted, the number of rows affected is: {}", ret);
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to delete node [{},{},{}] : {}", node.id(), node.ip(), node.port(), e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

void NodeDAO::assgin_nodes(const Node& node, shared_ptr<sql::PreparedStatement>& stmt, int& count)
{
  if (!node.id().empty()) {
    stmt->setString(count, node.id());
    count++;
  }

  if (!node.ip().empty()) {
    stmt->setString(count, node.ip());
    count++;
  }

  if (node.port() != 0) {
    stmt->setUInt(count, node.port());
    count++;
  }

  if (node.state() != UNDEFINED) {
    stmt->setUInt(count, node.state());
    count++;
  }

  if (node.metric() != nullptr) {
    stmt->setString(count, node.metric()->serialize_to_json());
    count++;
  }

  if (node.node_config() != nullptr) {
    stmt->setString(count, node.node_config()->serialize_to_json());
    count++;
  }

  if (!node.region().empty()) {
    stmt->setString(count, node.region());
    count++;
  }

  if (!node.zone().empty()) {
    stmt->setString(count, node.zone());
    count++;
  }

  if (node.incarnation() != 0) {
    stmt->setUInt64(count, node.incarnation());
    count++;
  }

  if (node.last_modify() != 0) {
    stmt->setUInt64(count, node.last_modify());
    count++;
  }

  if (!node.group().empty()) {
    stmt->setString(count, node.group());
    count++;
  }
}

string NodeDAO::nodes_where_clause_sql(const Node& node)
{
  string where_clause;
  int count = 0;

  if (!node.id().empty()) {
    where_clause += " id = ?";
    count++;
  }

  if (!node.ip().empty()) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " ip = ?";
    count++;
  }

  if (node.port() != 0) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " port = ?";
    count++;
  }

  if (node.state() != UNDEFINED) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " state = ?";
    count++;
  }

  if (node.metric() != nullptr) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " metric = ?";
    count++;
  }

  if (node.node_config() != nullptr) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " node_config = ?";
    count++;
  }

  if (!node.region().empty()) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " region = ?";
    count++;
  }

  if (!node.zone().empty()) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " zone = ?";
    count++;
  }

  if (node.incarnation() != 0) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " incarnation = ?";
    count++;
  }

  if (node.last_modify() != 0) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " last_modify = ?";
    count++;
  }

  if (!node.group().empty()) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " `group` = ?";
    count++;
  }

  if (count > 0) {
    where_clause = " WHERE " + where_clause;
  }

  return where_clause;
}

int NodeDAO::query_nodes(sql::Connection* conn, vector<Node*>& nodes, const set<State>& state_conditions)
{
  try {
    std::string sql = "SELECT * FROM nodes";
    std::string whereClause;
    int count = 0;

    if (!state_conditions.empty()) {
      whereClause += " WHERE state IN (";
      for (int i = 0; i < state_conditions.size(); i++) {
        if (i > 0) {
          whereClause += ", ";
        }
        whereClause += "?";
      }
      whereClause += ")";
      count += (int)state_conditions.size();
    }

    sql += whereClause;

    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    count = 1;
    if (!state_conditions.empty()) {
      for (const State& state : state_conditions) {
        stmt->setUInt(count, state);
        count++;
      }
    }

    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      return OMS_FAILED;
    }
    defer(delete res);
    while (res->next()) {
      Node* ret_node = new Node();
      convert_to_node(res, *ret_node);
      nodes.push_back(ret_node);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to query nodes by state: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

std::string NodeDAO::add_node_sql(const Node& node)
{
  int count = 0;
  string sql = "INSERT INTO nodes (";
  string values_clause = "VALUES (";
  if (!node.id().empty()) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "id";
    values_clause += "?";
    count++;
  }

  if (!node.ip().empty()) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "ip";
    values_clause += "?";
    count++;
  }

  if (node.port() != 0) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "port";
    values_clause += "?";
    count++;
  }

  if (node.state() != UNDEFINED) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "state";
    values_clause += "?";
    count++;
  }

  if (node.metric() != nullptr) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "metric";
    values_clause += "?";
    count++;
  }

  if (node.node_config() != nullptr) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "node_config";
    values_clause += "?";
    count++;
  }

  if (!node.region().empty()) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "region";
    values_clause += "?";
    count++;
  }

  if (!node.zone().empty()) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "zone";
    values_clause += "?";
    count++;
  }

  if (node.incarnation() != 0) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "incarnation";
    values_clause += "?";
    count++;
  }

  if (node.last_modify() != 0) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "last_modify";
    values_clause += "?";
    count++;
  }

  if (!node.group().empty()) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "`group`";
    values_clause += "?";
    count++;
  }

  sql += ") " + values_clause + ")";
  return sql;
}

int NodeDAO::update_node(sql::Connection* conn, const Node& node)
{
  try {
    std::string sql = "UPDATE nodes SET ";
    nodes_set_clause(node, sql);
    Node condition;
    condition.set_id(node.id());
    std::string where_clause = nodes_where_clause_sql(condition);
    sql += where_clause;
    OMS_DEBUG("Execute SQL:{}", sql);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assgin_nodes(node, stmt, count);
    assgin_nodes(condition, stmt, count);
    OMS_DEBUG("node info :{}", node.serialize_to_json());
    int ret = stmt->executeUpdate();
    OMS_DEBUG("Data was successfully updated, the number of rows affected is: {}", ret);
  } catch (sql::SQLException& e) {
    OMS_ERROR("Error update node [{},{},{}] : {}", node.id(), node.ip(), node.port(), e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int NodeDAO::query_node(sql::Connection* conn, Node& node)
{
  try {
    std::string sql = "SELECT * FROM nodes";
    sql += nodes_where_clause_sql(node);
    sql += " LIMIT 1";
    if (conn == nullptr) {
      OMS_ERROR("The connection is null and querying node information {} failed.", node.serialize_to_json());
      return OMS_FAILED;
    }
    std::shared_ptr<sql::PreparedStatement> statement(conn->prepareStatement(sql));
    int count = 1;
    assgin_nodes(node, statement, count);
    sql::ResultSet* res = statement->executeQuery();
    defer(delete res);
    if (res->rowsCount() != 1) {
      OMS_ERROR("Failed to query node information: {}, sql:{}, {}", node.serialize_to_json(), sql, res->rowsCount());
      return OMS_FAILED;
    }
    while (res->next()) {
      convert_to_node(res, node);
      OMS_DEBUG("query node info:{}", node.serialize_to_json());
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to selecting nodes: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

void NodeDAO::convert_to_node(const sql::ResultSet* res, Node& node)
{
  node.set_id(res->getString("id").c_str());
  node.set_ip(res->getString("ip").c_str());
  node.set_port(res->getUInt("port"));
  node.set_state(res->getInt("state"));
  node.set_last_modify(res->getUInt64("last_modify"));
  node.set_group(res->getString("group").c_str());

  auto* sys_metric = new SysMetric(true);
  sys_metric->deserialize_from_json(res->getString("metric").c_str());
  node.set_metric(sys_metric);

  node.set_region(res->getString("region").c_str());
  node.set_zone(res->getString("zone").c_str());

  auto* node_config = new NodeConfig();
  node_config->deserialize_from_json(res->getString("node_config").c_str());
  node.set_node_config(node_config);
}

int NodeDAO::query_nodes(sql::Connection* conn, set<std::string>& node_ids, map<std::string, Node>& nodes)
{
  std::string node_ids_str;
  for (const auto& id : node_ids) {
    node_ids_str = "'" + id + "',";
  }
  node_ids_str.pop_back();

  std::string sql = "SELECT * FROM nodes WHERE id IN (" + node_ids_str + ")";
  std::vector<Node> node_vec;
  if (OMS_OK != execute_multi_rows_query(conn, sql, node_vec, node_rs_converter)) {
    OMS_ERROR("Failed to query nodes, sql: {}", sql);
    return OMS_FAILED;
  }

  for (const auto& node : node_vec) {
    nodes[node.id()] = node;
  }
  return OMS_OK;
}

int BinlogInstanceDAO::query_all_instances(sql::Connection* conn, vector<BinlogEntry*>& binlog_instances)
{
  try {
    std::unique_ptr<sql::Statement> statement(conn->createStatement());
    sql::ResultSet* res = statement->executeQuery("SELECT * FROM binlog_instances");
    defer(delete res);
    // Loop through and print results
    while (res->next()) {
      auto* instance = new BinlogEntry();
      convert_to_instance(res, *instance);
      binlog_instances.push_back(instance);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to query instances: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int BinlogInstanceDAO::query_tenant_surviving_instances(
    sql::Connection* conn, string& cluster, string& tenant, std::vector<BinlogEntry>& instances)
{
  std::string sql = "SELECT * FROM binlog_instances WHERE cluster='" + cluster + "' AND tenant='" + tenant +
                    "' AND state != " + std::to_string(InstanceState::DROP) +
                    " AND state != " + std::to_string(InstanceState::LOGICAL_DROP);
  return execute_multi_rows_query(conn, sql, instances, instance_rs_converter);
}

int BinlogInstanceDAO::query_instance_by_entry(
    sql::Connection* conn, vector<BinlogEntry*>& binlog_instances, const BinlogEntry& condition)
{
  try {
    std::string sql = "SELECT * FROM binlog_instances";
    std::string where_clause;
    instance_where_clause_sql(condition, where_clause);

    sql += where_clause;
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_instance(condition, stmt, count);

    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      return OMS_FAILED;
    }
    defer(delete res);
    while (res->next()) {
      auto* instance = new BinlogEntry();
      convert_to_instance(res, *instance);
      binlog_instances.push_back(instance);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to select instances: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int BinlogInstanceDAO::downtime_instance_query(
    sql::Connection* conn, std::vector<BinlogEntry>& binlog_instances, std::string node_id)
{
  std::string sql = "SELECT * FROM binlog_instances WHERE node_id='" + node_id + "' AND state IN ( " +
                    std::to_string(InstanceState::OFFLINE) + " ," + std::to_string(InstanceState::FAILED) + ")";
  return execute_multi_rows_query(conn, sql, binlog_instances, instance_rs_converter);
}

void BinlogInstanceDAO::instance_where_clause_sql(const BinlogEntry& condition, string& where_clause)
{
  int count = 0;
  if (!condition.node_id().empty()) {
    where_clause += " node_id = ?";
    count++;
  }
  if (!condition.ip().empty()) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " ip = ?";
    count++;
  }
  if (condition.port() != 0) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " port = ?";
    count++;
  }
  if (!condition.cluster().empty()) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " cluster = ?";
    count++;
  }
  if (!condition.tenant().empty()) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " tenant = ?";
    count++;
  }
  if (condition.pid() != 0) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " pid = ?";
    count++;
  }
  if (!condition.work_path().empty()) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " work_path = ?";
    count++;
  }
  if (condition.state() != binlog::InstanceState::UNDEFINED) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " state = ?";
    count++;
  }
  if (condition.config() != nullptr) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " config = ?";
    count++;
  }
  if (condition.heartbeat() != 0) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " heartbeat = ?";
    count++;
  }
  if (condition.delay() != UINT64_MAX) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " delay = ?";
    count++;
  }

  if (!condition.instance_name().empty()) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " instance_name = ?";
    count++;
  }

  if (condition.min_dump_checkpoint() != 0) {
    if (count > 0) {
      where_clause += " AND";
    }
    where_clause += " min_dump_checkpoint = ?";
    count++;
  }

  if (count > 0) {
    where_clause = " WHERE " + where_clause;
  }
}

int BinlogInstanceDAO::add_instances(sql::Connection* conn, vector<BinlogEntry*>& binlog_instances)
{
  for (auto binlog_instance : binlog_instances) {
    if (add_instance(conn, *binlog_instance) != OMS_OK) {
      OMS_ERROR("Failed to add instance:{}", binlog_instance->serialize_to_json());
    }
  }
  return OMS_OK;
}

int BinlogInstanceDAO::add_instance(sql::Connection* conn, const BinlogEntry& binlog_instance)
{
  try {
    string sql = splice_add_instance_sql(binlog_instance);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_instance(binlog_instance, stmt, count);
    stmt->executeUpdate();
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to add instance: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

void BinlogInstanceDAO::assign_instance(const BinlogEntry& binlog_instance, shared_ptr<sql::PreparedStatement>& stmt,
    int& count, bool where, bool must_set_min_dump_checkpoint)
{
  if (!binlog_instance.node_id().empty()) {
    stmt->setString(count, binlog_instance.node_id());
    count++;
    OMS_DEBUG("set node_id={}", binlog_instance.node_id());
  }
  if (!binlog_instance.ip().empty()) {
    stmt->setString(count, binlog_instance.ip());
    count++;
    OMS_DEBUG("set ip={}", binlog_instance.ip());
  }
  if (binlog_instance.port() != 0) {
    stmt->setUInt(count, binlog_instance.port());
    count++;
    OMS_DEBUG("set port={}", binlog_instance.port());
  }
  if (!binlog_instance.cluster().empty()) {
    stmt->setString(count, binlog_instance.cluster());
    count++;
    OMS_DEBUG("set cluster={}", binlog_instance.cluster());
  }
  if (!binlog_instance.tenant().empty()) {
    stmt->setString(count, binlog_instance.tenant());
    count++;
    OMS_DEBUG("set tenant={}", binlog_instance.tenant());
  }
  if (binlog_instance.pid() != 0) {
    stmt->setUInt(count, binlog_instance.pid());
    count++;
    OMS_DEBUG("set pid={}", binlog_instance.pid());
  }
  if (!binlog_instance.work_path().empty()) {
    stmt->setString(count, binlog_instance.work_path());
    count++;
    OMS_DEBUG("set work_path={}", binlog_instance.work_path());
  }
  if (binlog_instance.state() != binlog::InstanceState::UNDEFINED) {
    stmt->setUInt(count, binlog_instance.state());
    count++;
    OMS_DEBUG("set state={}", binlog_instance.state());
  }
  if (binlog_instance.config() != nullptr && !where) {
    stmt->setString(count, binlog_instance.config()->serialize_to_json());
    count++;
    OMS_DEBUG("set config={}", binlog_instance.config()->serialize_to_json());
  }
  if (binlog_instance.heartbeat() != 0) {
    stmt->setUInt64(count, binlog_instance.heartbeat());
    count++;
    OMS_DEBUG("set heartbeat={}", binlog_instance.heartbeat());
  }
  if (binlog_instance.delay() != UINT64_MAX) {
    stmt->setUInt64(count, binlog_instance.delay());
    count++;
    OMS_DEBUG("set delay={}", binlog_instance.delay());
  }
  if (!binlog_instance.instance_name().empty()) {
    stmt->setString(count, binlog_instance.instance_name());
    count++;
    OMS_DEBUG("set instance_name={}", binlog_instance.instance_name());
  }

  if (must_set_min_dump_checkpoint || (!must_set_min_dump_checkpoint && binlog_instance.min_dump_checkpoint() != 0)) {
    stmt->setUInt64(count, binlog_instance.min_dump_checkpoint());
    count++;
    OMS_DEBUG("set min_dump_checkpoint={}", binlog_instance.min_dump_checkpoint());
  }
}

string BinlogInstanceDAO::splice_add_instance_sql(const BinlogEntry& binlog_instance)
{
  int count = 0;
  string sql = "INSERT INTO binlog_instances (";
  string values_clause = "VALUES (";
  if (!binlog_instance.node_id().empty()) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "node_id";
    values_clause += "?";
    count++;
  }
  if (!binlog_instance.ip().empty()) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "ip";
    values_clause += "?";
    count++;
  }
  if (binlog_instance.port() != 0) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "port";
    values_clause += "?";
    count++;
  }
  if (!binlog_instance.cluster().empty()) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "cluster";
    values_clause += "?";
    count++;
  }
  if (!binlog_instance.tenant().empty()) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "tenant";
    values_clause += "?";
    count++;
  }
  if (binlog_instance.pid() != 0) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "pid";
    values_clause += "?";
    count++;
  }
  if (!binlog_instance.work_path().empty()) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "work_path";
    values_clause += "?";
    count++;
  }
  if (binlog_instance.state() != binlog::InstanceState::UNDEFINED) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "state";
    values_clause += "?";
    count++;
  }
  if (binlog_instance.config() != nullptr) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "config";
    values_clause += "?";
    count++;
  }
  if (binlog_instance.heartbeat() != 0) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "heartbeat";
    values_clause += "?";
    count++;
  }
  if (binlog_instance.delay() != UINT64_MAX) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "delay";
    values_clause += "?";
    count++;
  }

  if (!binlog_instance.instance_name().empty()) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "instance_name";
    values_clause += "?";
    count++;
  }

  if (binlog_instance.min_dump_checkpoint() != 0) {
    if (count > 0) {
      sql += ",";
      values_clause += ",";
    }
    sql += "min_dump_checkpoint";
    values_clause += "?";
    count++;
  }

  sql += ") " + values_clause + ")";
  return sql;
}

int BinlogInstanceDAO::update_instance_state(sql::Connection* conn, const string& instance_name, InstanceState state)
{
  std::string sql =
      "UPDATE binlog_instances SET state = " + std::to_string(state) + " WHERE instance_name = '" + instance_name + "'";
  int32_t affected_rows = 0;
  return execute_update(conn, sql, affected_rows);
}

int BinlogInstanceDAO::update_instance(
    sql::Connection* conn, const BinlogEntry& binlog_instance, const BinlogEntry& condition)
{
  try {
    string sql = splice_update_instance_sql(binlog_instance, condition);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    OMS_DEBUG("update instance SQL: {}", sql);
    int count = 1;
    assign_instance(binlog_instance, stmt, count, false, true);
    assign_instance(condition, stmt, count, true);
    stmt->executeUpdate();
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to update instance: {},condition:{},reason:{}",
        binlog_instance.serialize_to_json(),
        condition.serialize_to_json(),
        e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

string BinlogInstanceDAO::splice_update_instance_sql(const BinlogEntry& binlog_instance, const BinlogEntry& condition)
{
  string sql = "UPDATE binlog_instances SET ";
  int count = 0;

  if (!binlog_instance.node_id().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "node_id = ?";
    count++;
  }
  if (!binlog_instance.ip().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "ip = ?";
    count++;
  }
  if (binlog_instance.port() != 0) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "port = ?";
    count++;
  }
  if (!binlog_instance.cluster().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "cluster = ?";
    count++;
  }
  if (!binlog_instance.tenant().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "tenant = ?";
    count++;
  }
  if (binlog_instance.pid() != 0) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "pid = ?";
    count++;
  }
  if (!binlog_instance.work_path().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "work_path = ?";
    count++;
  }
  if (binlog_instance.state() != binlog::InstanceState::UNDEFINED) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "state = ?";
    count++;
  }
  if (binlog_instance.config() != nullptr) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "config = ?";
    count++;
  }
  if (binlog_instance.heartbeat() != 0) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "heartbeat = ?";
    count++;
  }
  if (binlog_instance.delay() != UINT64_MAX) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "delay = ?";
    count++;
  }
  if (!binlog_instance.instance_name().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "instance_name = ?";
    count++;
  }

  if (count > 0) {
    sql += ", ";
  }
  sql += "min_dump_checkpoint = ?";
  count++;

  vector<string> conditions;
  if (!condition.node_id().empty()) {
    conditions.emplace_back("node_id = ?");
  }
  if (!condition.ip().empty()) {
    conditions.emplace_back("ip = ?");
  }
  if (condition.port() != 0) {
    conditions.emplace_back("port = ?");
  }
  if (!condition.cluster().empty()) {
    conditions.emplace_back("cluster = ?");
  }
  if (!condition.tenant().empty()) {
    conditions.emplace_back("tenant = ?");
  }
  if (condition.pid() != 0) {
    conditions.emplace_back("pid = ?");
  }
  if (!condition.work_path().empty()) {
    conditions.emplace_back("work_path = ?");
  }
  if (condition.state() != binlog::InstanceState::UNDEFINED) {
    conditions.emplace_back("state = ?");
  }
  if (condition.heartbeat() != 0) {
    conditions.emplace_back("heartbeat = ?");
  }
  if (condition.delay() != UINT64_MAX) {
    conditions.emplace_back("delay = ?");
  }
  if (!condition.instance_name().empty()) {
    conditions.emplace_back("instance_name = ?");
  }
  if (condition.min_dump_checkpoint() != 0) {
    conditions.emplace_back("min_dump_checkpoint = ?");
  }

  if (!conditions.empty()) {
    sql += " WHERE ";
    sql += conditions[0];
    for (int i = 1; i < conditions.size(); i++) {
      sql += " AND " + conditions[i];
    }
  }
  return sql;
}

int BinlogInstanceDAO::delete_instances(sql::Connection* conn, vector<BinlogEntry*>& binlog_instances)
{
  return 0;
}

int BinlogInstanceDAO::query_instances(
    sql::Connection* conn, vector<BinlogEntry*>& instances, const set<std::string>& instance_names)
{
  try {
    std::string sql = "SELECT * FROM binlog_instances";
    std::string whereClause;
    int count = 0;

    if (!instance_names.empty()) {
      whereClause += " WHERE instance_name IN (";
      for (int i = 0; i < instance_names.size(); i++) {
        if (i > 0) {
          whereClause += ", ";
        }
        whereClause += "?";
      }
      whereClause += ")";
      count += (int)instance_names.size();
    }

    sql += whereClause;

    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    count = 1;
    if (!instance_names.empty()) {
      for (const std::string& name : instance_names) {
        stmt->setString(count, name);
        count++;
      }
    }

    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      return OMS_FAILED;
    }
    defer(delete res);
    while (res->next()) {
      auto* instance = new BinlogEntry();
      convert_to_instance(res, *instance);
      instances.push_back(instance);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Error query nodes by state: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int BinlogInstanceDAO::query_instances(
    sql::Connection* conn, vector<BinlogEntry*>& instances, const set<InstanceState>& instance_states)
{
  try {
    std::string sql = "SELECT * FROM binlog_instances";
    std::string whereClause;
    int count = 0;

    if (!instance_states.empty()) {
      whereClause += " WHERE state IN (";
      for (int i = 0; i < instance_states.size(); i++) {
        if (i > 0) {
          whereClause += ", ";
        }
        whereClause += "?";
      }
      whereClause += ")";
      count += (int)instance_states.size();
    }

    sql += whereClause;

    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    count = 1;
    if (!instance_states.empty()) {
      for (const InstanceState& state : instance_states) {
        stmt->setUInt(count, state);
        count++;
      }
    }

    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      return OMS_FAILED;
    }
    defer(delete res);
    while (res->next()) {
      auto* instance = new BinlogEntry();
      convert_to_instance(res, *instance);
      instances.push_back(instance);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to query nodes by state: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int BinlogInstanceDAO::query_instance(sql::Connection* conn, BinlogEntry& instance, const string& instance_name)
{
  try {
    std::string sql = "SELECT * FROM binlog_instances WHERE instance_name = ? LIMIT 1";

    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    stmt->setString(1, instance_name);

    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      OMS_ERROR("Failed to query instance {} after retry 3 times", instance_name);
      return OMS_FAILED;
    }
    defer(delete res);
    while (res->next()) {
      convert_to_instance(res, instance);
      OMS_DEBUG("query instance:{}", instance.serialize_to_json());
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to query instance by name: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

void BinlogInstanceDAO::convert_to_instance(const sql::ResultSet* res, BinlogEntry& instance)
{
  instance.set_node_id(res->getString("node_id").c_str());
  instance.set_ip(res->getString("ip").c_str());
  instance.set_port(res->getInt("port"));
  instance.set_cluster(res->getString("cluster").c_str());
  instance.set_tenant(res->getString("tenant").c_str());
  instance.set_pid(res->getInt("pid"));
  instance.set_work_path(res->getString("work_path").c_str());
  instance.set_state(res->getInt("state"));
  auto* instance_config = new InstanceMeta(true);
  instance_config->deserialize_from_json(res->getString("config").c_str());
  instance.set_config(instance_config);
  instance.set_heartbeat(res->getLong("heartbeat"));
  instance.set_delay(res->getLong("delay"));
  instance.set_min_dump_checkpoint(res->getLong("min_dump_checkpoint"));
  instance.set_instance_name(res->getString("instance_name").c_str());
  instance.set_cluster_id(instance_config->cluster_id());
  instance.set_tenant_id(instance_config->tenant_id());
}

int BinlogInstanceDAO::query_instances_by_node(sql::Connection* conn, vector<BinlogEntry*>& instances, const Node& node)
{
  try {
    std::string sql =
        "SELECT binlog_instances.* FROM binlog_instances JOIN nodes ON nodes.id = binlog_instances.node_id ";

    string where_clause;
    int count = 0;

    if (!node.id().empty()) {
      where_clause += " nodes.id = ?";
      count++;
    }

    if (!node.ip().empty()) {
      if (count > 0) {
        where_clause += " AND";
      }
      where_clause += " nodes.ip = ?";
      count++;
    }

    if (node.port() != 0) {
      if (count > 0) {
        where_clause += " AND";
      }
      where_clause += " nodes.port = ?";
      count++;
    }

    if (node.state() != UNDEFINED) {
      if (count > 0) {
        where_clause += " AND";
      }
      where_clause += " nodes.state = ?";
      count++;
    }

    if (node.metric() != nullptr) {
      if (count > 0) {
        where_clause += " AND";
      }
      where_clause += " nodes.metric = ?";
      count++;
    }

    if (node.node_config() != nullptr) {
      if (count > 0) {
        where_clause += " AND";
      }
      where_clause += " nodes.node_config = ?";
      count++;
    }

    if (!node.region().empty()) {
      if (count > 0) {
        where_clause += " AND";
      }
      where_clause += " nodes.region = ?";
      count++;
    }

    if (!node.zone().empty()) {
      if (count > 0) {
        where_clause += " AND";
      }
      where_clause += " nodes.zone = ?";
      count++;
    }

    if (node.incarnation() != 0) {
      if (count > 0) {
        where_clause += " AND";
      }
      where_clause += " nodes.incarnation = ?";
      count++;
    }

    if (node.last_modify() != 0) {
      if (count > 0) {
        where_clause += " AND";
      }
      where_clause += " nodes.last_modify = ?";
      count++;
    }

    if (!node.group().empty()) {
      if (count > 0) {
        where_clause += " AND";
      }
      where_clause += " nodes.group = ?";
      count++;
    }

    if (count > 0) {
      where_clause = " WHERE " + where_clause;
    }
    sql += where_clause;

    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    count = 1;
    NodeDAO::assgin_nodes(node, stmt, count);

    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      return OMS_FAILED;
    }
    defer(delete res);
    while (res->next()) {
      auto* instance = new BinlogEntry();
      convert_to_instance(res, *instance);
      instances.push_back(instance);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to query instance by name: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int BinlogInstanceDAO::update_instance(
    sql::Connection* conn, const BinlogEntry& binlog_instance, const string& instance_name)
{
  BinlogEntry condition;
  condition.set_instance_name(instance_name);
  return BinlogInstanceDAO::update_instance(conn, binlog_instance, condition);
}

int BinlogInstanceDAO::query_instance_by_row_lock(
    sql::Connection* conn, vector<BinlogEntry*>& binlog_instances, const BinlogEntry& condition)
{
  try {
    std::string sql = "SELECT * FROM binlog_instances";
    std::string where_clause;
    instance_where_clause_sql(condition, where_clause);

    sql += where_clause;
    sql += " FOR UPDATE";
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_instance(condition, stmt, count);

    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      return OMS_FAILED;
    }
    defer(delete res);
    while (res->next()) {
      auto* instance = new BinlogEntry();
      convert_to_instance(res, *instance);
      binlog_instances.push_back(instance);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to select instances: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int TaskDAO::query_all_tasks(sql::Connection* conn, vector<Task*>& tasks)
{
  try {
    string sql = "SELECT * FROM tasks";
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      return OMS_FAILED;
    }
    defer(delete res);
    while (res->next()) {
      auto* ret_task = new Task();
      convert_to_task(res, *ret_task);
      tasks.push_back(ret_task);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to query all task,reason:{}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int TaskDAO::query_tasks_by_entry(sql::Connection* conn, vector<Task*>& tasks, const Task& condition)
{
  try {
    std::string sql = "SELECT * FROM tasks ";
    std::string where_clause;
    task_where(condition, where_clause);
    sql += where_clause;
    OMS_DEBUG("Query all tasks,sql: {}", sql);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_task(condition, stmt, count);
    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      return OMS_FAILED;
    }
    defer(delete res);
    while (res->next()) {
      Task* ret_task = new Task();
      convert_to_task(res, *ret_task);
      tasks.push_back(ret_task);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to query all task,reason:{}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int TaskDAO::add_tasks(sql::Connection* conn, const vector<Task>& tasks)
{
  for (auto task : tasks) {
    if (add_task(conn, task) != OMS_OK) {
      OMS_ERROR("Failed to add task:{}", task.serialize_to_json());
    }
  }
  return OMS_OK;
}

int TaskDAO::add_task(sql::Connection* conn, Task& task)
{
  try {
    if (task.task_id().empty()) {
      task.set_task_id(binlog::CommonUtils::generate_trace_id());
    }

    if (task.last_modify() == 0) {
      task.set_last_modify(Timer::now_s());
    }

    string sql = add_task_sql(task);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_task(task, stmt, count);
    // stmt->executeUpdate();
    int32_t affected_rows = 0;
    if (execute_update_with_retry(stmt, affected_rows) != OMS_OK) {
      return OMS_FAILED;
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to add task: {}, error: {}", task.serialize_to_json(), e.what());
    return OMS_FAILED;
  }
  OMS_INFO("Successfully added task: {}", task.serialize_to_json());
  return OMS_OK;
}

void TaskDAO::assign_task(const Task& task, shared_ptr<sql::PreparedStatement>& stmt, int& count)
{
  if (!task.task_id().empty()) {
    stmt->setString(count, task.task_id());
    count++;
    OMS_DEBUG("set task_id={}", task.task_id());
  }

  if (task.type() != TASK_TYPE_UNDEF) {
    stmt->setInt(count, task.type());
    count++;
    OMS_DEBUG("set type={}", task.type());
  }

  if (task.status() != UNDEF) {
    stmt->setInt(count, task.status());
    count++;
    OMS_DEBUG("set status={}", task.status());
  }

  if (!task.execution_node().empty()) {
    stmt->setString(count, task.execution_node());
    count++;
    OMS_DEBUG("set execution_node={}", task.execution_node());
  }

  if (task.retry_count() != 0) {
    stmt->setUInt(count, task.retry_count());
    count++;
    OMS_DEBUG("set retry_count={}", task.retry_count());
  }

  if (task.task_param() != nullptr) {
    stmt->setString(count, task.task_param()->serialize_to_json());
    count++;
    OMS_DEBUG("set task_param={}", task.task_param()->serialize_to_json());
  }

  if (task.last_modify() != 0) {
    stmt->setUInt64(count, task.last_modify());
    count++;
    OMS_DEBUG("set last_modify={}", task.last_modify());
  }

  if (!task.instance_name().empty()) {
    stmt->setString(count, task.instance_name());
    count++;
    OMS_DEBUG("set instance_name={}", task.instance_name());
  }
}

string TaskDAO::add_task_sql(const Task& task)
{
  string sql = "INSERT INTO tasks (";
  string values = " VALUES (";
  int count = 0;

  if (!task.task_id().empty()) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "task_id";
    values += "?";
    count++;
  }
  if (task.type() != TASK_TYPE_UNDEF) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "type";
    values += "?";
    count++;
  }
  if (task.status() != UNDEF) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "status";
    values += "?";
    count++;
  }
  if (!task.execution_node().empty()) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "execution_node";
    values += "?";
    count++;
  }
  if (task.retry_count() != 0) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "retry_count";
    values += "?";
    count++;
  }
  if (task.task_param() != nullptr) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "task_param";
    values += "?";
    count++;
  }

  if (task.last_modify() != 0) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "last_modify";
    values += "?";
    count++;
  }

  if (!task.instance_name().empty()) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "instance_name";
    values += "?";
    count++;
  }

  sql += ")";
  values += ")";

  sql += values;
  return sql;
}

int TaskDAO::update_task(sql::Connection* conn, Task& task, const Task& condition)
{
  try {
    if (task.last_modify() == 0) {
      task.set_last_modify(Timer::now_s());
    }
    string sql = update_task_sql(task, condition);
    OMS_DEBUG("update_task sql:{}", sql);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_task(task, stmt, count);
    assign_task(condition, stmt, count);
    int32_t affected_rows = 0;
    return execute_update_with_retry(stmt, affected_rows);
    // stmt->executeUpdate();
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to update task: {},reason:{}", task.serialize_to_json(), e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

string TaskDAO::update_task_sql(const Task& task, const Task& condition)
{
  string sql = "UPDATE tasks SET ";
  int count = 0;

  if (!task.task_id().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "task_id = ?";
    count++;
  }
  if (task.type() != TASK_TYPE_UNDEF) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "type = ?";
    count++;
  }
  if (task.status() != UNDEF) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "status = ?";
    count++;
  }
  if (!task.execution_node().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "execution_node = ?";
    count++;
  }
  if (task.retry_count() != 0) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "retry_count = ?";
    count++;
  }

  if (task.task_param() != nullptr) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "task_param = ?";
    count++;
  }

  if (task.last_modify() != 0) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "last_modify = ?";
    count++;
  }

  if (!task.instance_name().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "instance_name = ?";
    count++;
  }

  std::string where_clause;
  task_where(condition, where_clause);
  sql += where_clause;
  return sql;
}

int TaskDAO::delete_task(sql::Connection* conn, const Task& task)
{
  try {
    std::string sql = "DELETE FROM tasks ";
    std::string where_clause;
    task_where(task, where_clause);
    sql += where_clause;
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_task(task, stmt, count);
    stmt->executeUpdate();
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to delete task: {},reason:{}", task.serialize_to_json(), e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

void TaskDAO::task_where(const Task& task, string& sql)
{
  int count = 0;

  if (!task.task_id().empty()) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "task_id = ?";
    count++;
  }
  if (task.type() != TASK_TYPE_UNDEF) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "type = ?";
    count++;
  }
  if (task.status() != UNDEF) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "status = ?";
    count++;
  }
  if (!task.execution_node().empty()) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "execution_node = ?";
    count++;
  }
  if (task.retry_count() != 0) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "retry_count = ?";
    count++;
  }
  if (task.task_param() != nullptr) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "task_param = ?";
    count++;
  }

  if (task.last_modify() != 0) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "last_modify = ?";
    count++;
  }

  if (!task.instance_name().empty()) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "instance_name = ?";
    count++;
  }

  if (count > 0) {
    sql = " WHERE " + sql;
  }
}

int TaskDAO::delete_tasks(sql::Connection* conn, vector<Task>& tasks)
{
  return 0;
}

int TaskDAO::query_task_by_entry(sql::Connection* conn, Task& task, const Task& condition)
{
  try {
    std::string sql = "SELECT * FROM tasks ";
    std::string where_clause;
    task_where(condition, where_clause);
    sql += where_clause;
    OMS_DEBUG("Query all tasks,sql: {}", sql);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_task(condition, stmt, count);
    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      return OMS_FAILED;
    }
    defer(delete res);
    OMS_DEBUG("Query the number of rows:{}", res->rowsCount());
    while (res->next()) {
      convert_to_task(res, task);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to query all task,reason:{}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

void TaskDAO::convert_to_task(const sql::ResultSet* res, Task& task)
{
  task.set_task_id(res->getString("task_id").c_str());
  task.set_instance_name(res->getString("instance_name").c_str());
  task.set_type(res->getUInt("type"));
  task.set_execution_node(res->getString("execution_node").c_str());
  task.set_retry_count(res->getInt("retry_count"));
  task.set_status(res->getUInt("status"));
  task.set_last_modify(res->getUInt64("last_modify"));

  switch (task.type()) {
    case TaskType::CREATE_INSTANCE: {
      auto* task_param = new CreateInstanceTaskParam();
      task_param->deserialize_from_json(res->getString("task_param").c_str());
      task.set_task_param(task_param);
      break;
    }
    case DROP_INSTANCE: {
      auto* task_param = new DropInstanceTaskParam();
      task_param->deserialize_from_json(res->getString("task_param").c_str());
      task.set_task_param(task_param);
      break;
    }
    case TaskType::STOP_INSTANCE: {
      auto* task_param = new StopInstanceTaskParam();
      task_param->deserialize_from_json(res->getString("task_param").c_str());
      task.set_task_param(task_param);
      break;
    }
    case TaskType::START_INSTANCE: {
      auto* task_param = new StartInstanceTaskParam();
      task_param->deserialize_from_json(res->getString("task_param").c_str());
      task.set_task_param(task_param);
      break;
    }
    case TaskType::RECOVER_INSTANCE: {
      auto* task_param = new RecoverInstanceTaskParam();
      task_param->deserialize_from_json(res->getString("task_param").c_str());
      task.set_task_param(task_param);
      break;
    }
    default: {
      OMS_ERROR("Unsupported task type: {}", task.type());
      break;
    }
  }
}

int ConfigTemplateDAO::query_config_item(sql::Connection* conn, ConfigTemplate& config, const ConfigTemplate& condition)
{
  try {
    std::string sql = "SELECT * FROM config_template ";
    std::string where_clause;
    config_item_where(condition, where_clause, true);
    sql += where_clause;
    OMS_DEBUG("Query config item,sql: {}", sql);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_config_item(condition, stmt, count, true);
    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      return OMS_FAILED;
    }
    defer(delete res);
    OMS_DEBUG("Query the number of rows:{}", res->rowsCount());
    while (res->next()) {
      convert_to_config_item(res, config);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to query config item:{},reason:{}", config.serialize_to_json(), e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int ConfigTemplateDAO::query_config_items(
    sql::Connection* conn, std::vector<ConfigTemplate*>& configs, const ConfigTemplate& condition)
{
  try {
    std::string sql = "SELECT * FROM config_template ";
    std::string where_clause;
    config_item_where(condition, where_clause, true);
    sql += where_clause;
    OMS_DEBUG("Query config items,sql: {}", sql);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_config_item(condition, stmt, count, true);
    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      return OMS_FAILED;
    }
    defer(delete res);
    while (res->next()) {
      auto* ret_config_template = new ConfigTemplate();
      convert_to_config_item(res, ret_config_template);
      configs.push_back(ret_config_template);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to query config items,reason:{}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int ConfigTemplateDAO::query_config_items(
    sql::Connection* conn, std::map<std::string, std::string>& configs, const ConfigTemplate& condition)
{
  try {
    std::string sql = "SELECT * FROM config_template ";
    std::string where_clause;
    config_item_where(condition, where_clause, true);
    sql += where_clause;
    OMS_DEBUG("Query config items,sql: {}", sql);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_config_item(condition, stmt, count, true);
    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      return OMS_FAILED;
    }
    defer(delete res);
    while (res->next()) {
      configs[res->getString("key_name").c_str()] = res->getString("value");
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to query config items,reason:{}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int ConfigTemplateDAO::update_config_item(
    sql::Connection* conn, ConfigTemplate& config, const ConfigTemplate& condition)
{
  try {
    string sql = update_config_item_sql(config, condition);
    OMS_DEBUG("update config item sql:{}", sql);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_config_item(config, stmt, count);
    assign_config_item(condition, stmt, count);
    stmt->executeUpdate();
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to update config: {},reason:{}", config.serialize_to_json(), e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int ConfigTemplateDAO::add_config_item(sql::Connection* conn, ConfigTemplate& config, const ConfigTemplate& condition)
{
  try {
    string sql = add_config_item_sql(config);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_config_item(config, stmt, count);
    stmt->executeUpdate();
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to add config item: {},reason:{}", config.serialize_to_json(), e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

string ConfigTemplateDAO::add_config_item_sql(const ConfigTemplate& config)
{
  string sql = "INSERT INTO config_template (";
  string values = " VALUES (";
  int count = 0;

  if (config.id() != 0) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "id";
    values += "?";
    count++;
  }

  if (!config.version().empty()) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "version";
    values += "?";
    count++;
  }

  if (!config.key_name().empty()) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "key_name";
    values += "?";
    count++;
  }

  if (count > 0) {
    sql += ", ";
    values += ", ";
  }
  sql += "value";
  values += "?";
  count++;

  if (config.granularity() != G_UNDEFINED) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "granularity";
    values += "?";
    count++;
  }

  if (!config.scope().empty()) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "scope";
    values += "?";
    count++;
  }

  sql += ")";
  values += ")";

  sql += values;
  return sql;
}

void ConfigTemplateDAO::assign_config_item(
    const ConfigTemplate& config, shared_ptr<sql::PreparedStatement>& stmt, int& count, bool query)
{
  if (config.id() != 0) {
    stmt->setUInt64(count, config.id());
    count++;
    OMS_DEBUG("set id={}", config.id());
  }

  if (!config.version().empty()) {
    stmt->setString(count, config.version());
    count++;
    OMS_DEBUG("set version={}", config.version());
  }

  if (!config.key_name().empty()) {
    stmt->setString(count, config.key_name());
    count++;
    OMS_DEBUG("set key_name={}", config.key_name());
  }
  if (!config.value().empty() || !query) {
    stmt->setString(count, config.value());
    count++;
    OMS_DEBUG("set value={}", config.value());
  }
  if (config.granularity() != G_UNDEFINED) {
    stmt->setUInt(count, config.granularity());
    count++;
    OMS_DEBUG("set granularity={}", config.granularity());
  }

  if (!config.scope().empty()) {
    stmt->setString(count, config.scope());
    count++;
    OMS_DEBUG("set scope={}", config.scope());
  }
}

string ConfigTemplateDAO::update_config_item_sql(const ConfigTemplate& config, const ConfigTemplate& condition)
{
  string sql = "UPDATE config_template SET ";
  int count = 0;

  if (config.id() != 0) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "id = ?";
    count++;
  }

  if (!config.version().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "version = ?";
    count++;
  }

  if (!config.key_name().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "key_name = ?";
    count++;
  }

  if (count > 0) {
    sql += ", ";
  }
  sql += "value = ?";
  count++;

  if (config.granularity() != G_UNDEFINED) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "granularity = ?";
    count++;
  }

  if (!config.scope().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "scope = ?";
    count++;
  }

  std::string where_clause;
  config_item_where(condition, where_clause);
  sql += where_clause;
  return sql;
}

void ConfigTemplateDAO::config_item_where(const ConfigTemplate& config, string& sql, bool query)
{
  int count = 0;

  if (config.id() != 0) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "id = ?";
    count++;
  }

  if (!config.version().empty()) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "version = ?";
    count++;
  }

  if (!config.key_name().empty()) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "key_name = ?";
    count++;
  }

  if (!config.value().empty() || !query) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "value = ?";
    count++;
  }

  if (config.granularity() != G_UNDEFINED) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "granularity = ?";
    count++;
  }

  if (!config.scope().empty()) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "scope = ?";
    count++;
  }

  if (count > 0) {
    sql = " WHERE " + sql;
  }
}

void ConfigTemplateDAO::convert_to_config_item(const sql::ResultSet* res, ConfigTemplate* config)
{
  config->set_id(res->getUInt64("id"));
  config->set_version(res->getString("version").c_str());
  config->set_key_name(res->getString("key_name").c_str());
  config->set_value(res->getString("value").c_str());
  config->set_granularity(res->getUInt("granularity"));
  config->set_scope(res->getString("scope").c_str());
}

void ConfigTemplateDAO::convert_to_config_item(const sql::ResultSet* res, ConfigTemplate& config)
{
  config.set_id(res->getUInt64("id"));
  config.set_version(res->getString("version").c_str());
  config.set_key_name(res->getString("key_name").c_str());
  config.set_value(res->getString("value").c_str());
  config.set_granularity(res->getUInt("granularity"));
  config.set_scope(res->getString("scope").c_str());
}

int ConfigTemplateDAO::replace_into_config_item(sql::Connection* conn, ConfigTemplate& config)
{
  try {
    string sql = replace_into_config_item_sql(config);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_config_item(config, stmt, count);
    stmt->executeUpdate();
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to add config item: {},reason:{}", config.serialize_to_json(), e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

string ConfigTemplateDAO::replace_into_config_item_sql(const ConfigTemplate& config)
{
  string sql = "REPLACE INTO config_template (";
  string values = " VALUES (";
  int count = 0;

  if (config.id() != 0) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "id";
    values += "?";
    count++;
  }

  if (!config.version().empty()) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "version";
    values += "?";
    count++;
  }

  if (!config.key_name().empty()) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "key_name";
    values += "?";
    count++;
  }

  if (count > 0) {
    sql += ", ";
    values += ", ";
  }
  sql += "value";
  values += "?";
  count++;

  if (config.granularity() != G_UNDEFINED) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "granularity";
    values += "?";
    count++;
  }

  if (!config.scope().empty()) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "scope";
    values += "?";
    count++;
  }

  sql += ")";
  values += ")";

  sql += values;
  return sql;
}

int ConfigTemplateDAO::delete_config_item(sql::Connection* conn, ConfigTemplate& config)
{
  try {
    std::string sql = "DELETE FROM config_template ";
    string where_clause;
    config_item_where(config, where_clause, true);
    sql += where_clause;
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_config_item(config, stmt, count, true);
    int ret = stmt->executeUpdate();
    OMS_DEBUG(
        "config :{} was successfully deleted, the number of rows affected is: {}", config.serialize_to_json(), ret);
  } catch (sql::SQLException& e) {
    OMS_ERROR(
        "Failed to delete config [{},{},{}] : {}", config.key_name(), config.granularity(), config.scope(), e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int update_instance_gtid_seq(sql::Connection* conn, const BinlogEntry& instance,
    const std::pair<InstanceGtidSeq, InstanceGtidSeq>& boundary_gtid_seq, const uint64_t last_purged_gtid,
    const std::string& incr_gtid_seq_str, uint64_t last_purged_checkpoint)
{
  OMS_DEBUG("[{}]: update instance gtid seq: {}, last_purged_gtid: {}, last_purged_checkpoint: {}",
      instance.instance_name(),
      incr_gtid_seq_str,
      last_purged_gtid,
      last_purged_checkpoint);
  if (!incr_gtid_seq_str.empty()) {
    std::vector<InstanceGtidSeq> gtid_seq_vec;
    deserialize_gtid_seq_str(instance, incr_gtid_seq_str, gtid_seq_vec);
    if (gtid_seq_vec.front().commit_version_start() > boundary_gtid_seq.second.commit_version_start()) {
      if (OMS_OK != InstanceGtidSeqDAO::add_gtid_seq_batch(conn, gtid_seq_vec)) {
        OMS_ERROR("Failed to add gtid seq [{}] for instance [{}].", incr_gtid_seq_str, instance.instance_name());
        return OMS_FAILED;
      }
    }
  }

  if (last_purged_gtid > boundary_gtid_seq.first.gtid_start()) {
    if (OMS_OK !=
        InstanceGtidSeqDAO::remove_instance_gtid_seq_before_gtid(conn, instance.instance_name(), last_purged_gtid)) {
      OMS_ERROR("Failed to remove last purged gtid [{}] of instance [{}] with min gtid [{}]",
          last_purged_gtid,
          instance.instance_name(),
          boundary_gtid_seq.first.gtid_start());
      return OMS_FAILED;
    }
  }

  if (last_purged_checkpoint > boundary_gtid_seq.first.commit_version_start() && instance.heartbeat() % 600 < 5) {
    OMS_DEBUG("Begin to purge checkpoint: {}", last_purged_checkpoint);
    if (OMS_OK != InstanceGtidSeqDAO::remove_instance_gtid_seq_before_checkpoint(
                      conn, instance.instance_name(), last_purged_checkpoint)) {
      OMS_ERROR("Failed to remove last purged checkpoint [{}] of instance [{}] with min gtid [{}]",
          last_purged_checkpoint,
          instance.instance_name(),
          boundary_gtid_seq.first.gtid_start());
      return OMS_FAILED;
    }
  }
  return OMS_OK;
}

void mark_dump_metric(const BinlogEntry& instance, InstanceClient& client, MySQLResultSet& rs)
{
  std::string show_sql = "SHOW FULL BINLOG DUMPLIST";
  rs.reset();
  if (OMS_OK != client.query(show_sql, rs)) {
    OMS_ERROR("Failed to show binlog dumplist for instance:{}", instance.instance_name());
    return;
  }

  for (const MySQLRow& row : rs.rows) {
    auto trace_id = row.fields()[0];
    PrometheusExposer::mark_binlog_dump_metric(instance.node_id(),
        instance.instance_name(),
        instance.cluster(),
        instance.tenant(),
        trace_id,
        row.fields()[1],
        row.fields()[2],
        instance.cluster_id(),
        instance.tenant_id(),
        std::stod(row.fields()[8].c_str()),
        BINLOG_INSTANCE_DUMP_DELAY_TYPE);
    PrometheusExposer::mark_binlog_dump_metric(instance.node_id(),
        instance.instance_name(),
        instance.cluster(),
        instance.tenant(),
        trace_id,
        row.fields()[1],
        row.fields()[2],
        instance.cluster_id(),
        instance.tenant_id(),
        std::stod(row.fields()[9].c_str()),
        BINLOG_INSTANCE_DUMP_EVENT_NUM_TYPE);
    PrometheusExposer::mark_binlog_dump_metric(instance.node_id(),
        instance.instance_name(),
        instance.cluster(),
        instance.tenant(),
        trace_id,
        row.fields()[1],
        row.fields()[2],
        instance.cluster_id(),
        instance.tenant_id(),
        std::stod(row.fields()[10].c_str()),
        BINLOG_INSTANCE_DUMP_EVENT_BYTES_TYPE);
    PrometheusExposer::mark_binlog_dump_metric(instance.node_id(),
        instance.instance_name(),
        instance.cluster(),
        instance.tenant(),
        trace_id,
        row.fields()[1],
        row.fields()[2],
        instance.cluster_id(),
        instance.tenant_id(),
        std::stod(row.fields()[12].c_str()),
        BINLOG_INSTANCE_DUMP_HEARTBEAT_RPS_TYPE);
    PrometheusExposer::mark_binlog_dump_metric(instance.node_id(),
        instance.instance_name(),
        instance.cluster(),
        instance.tenant(),
        trace_id,
        row.fields()[1],
        row.fields()[2],
        instance.cluster_id(),
        instance.tenant_id(),
        std::stod(row.fields()[13].c_str()),
        BINLOG_INSTANCE_DUMP_RPS_TYPE);
    PrometheusExposer::mark_binlog_dump_metric(instance.node_id(),
        instance.instance_name(),
        instance.cluster(),
        instance.tenant(),
        trace_id,
        row.fields()[1],
        row.fields()[2],
        instance.cluster_id(),
        instance.tenant_id(),
        std::stod(row.fields()[14].c_str()),
        BINLOG_INSTANCE_DUMP_IOPS_TYPE);

    PrometheusExposer::mark_binlog_dump_metric(instance.node_id(),
        instance.instance_name(),
        instance.cluster(),
        instance.tenant(),
        trace_id,
        row.fields()[1],
        row.fields()[2],
        instance.cluster_id(),
        instance.tenant_id(),
        row.fields().size() > 16 ? std::stod(row.fields()[16].c_str())
                                 : Timer::now_s() - std::stod(row.fields()[8].c_str()),
        BINLOG_INSTANCE_DUMP_CHECKPOINT_TYPE);
  }
  PrometheusExposer::mark_binlog_dump_metric(instance.node_id(),
      instance.instance_name(),
      instance.cluster(),
      instance.tenant(),
      "",
      "",
      "",
      instance.cluster_id(),
      instance.tenant_id(),
      rs.rows.size(),
      BINLOG_INSTANCE_DUMP_COUNT_TYPE);
}

void mark_converter_metric(const BinlogEntry& instance, MySQLResultSet& rs)
{
  auto row = rs.rows.front();
  double convert_delay = std::stod(rs.rows.front().fields()[1].c_str());
  double convert_rps = std::stod(rs.rows.front().fields()[2].c_str());
  double convert_eps = std::stod(rs.rows.front().fields()[3].c_str());
  double convert_iops = std::stod(rs.rows.front().fields()[4].c_str());
  double convert_checkpoint =
      rs.rows.front().fields().size() >= 10 ? std::stod(rs.rows.front().fields()[9].c_str()) : 0;
  double dump_error_count = rs.rows.front().fields().size() >= 11 ? std::stod(rs.rows.front().fields()[10].c_str()) : 0;
  PrometheusExposer::mark_binlog_instance_metric(instance.instance_name(),
      instance.cluster(),
      instance.tenant(),
      instance.cluster_id(),
      instance.tenant_id(),
      convert_delay,
      BINLOG_INSTANCE_CONVERT_DELAY_TYPE);
  PrometheusExposer::mark_binlog_instance_metric(instance.instance_name(),
      instance.cluster(),
      instance.tenant(),
      instance.cluster_id(),
      instance.tenant_id(),
      convert_checkpoint,
      BINLOG_INSTANCE_CONVERT_CHECKPOINT_TYPE);

  PrometheusExposer::mark_binlog_instance_metric(instance.instance_name(),
      instance.cluster(),
      instance.tenant(),
      instance.cluster_id(),
      instance.tenant_id(),
      convert_iops,
      BINLOG_INSTANCE_CONVERT_IOPS_TYPE);
  PrometheusExposer::mark_binlog_instance_metric(instance.instance_name(),
      instance.cluster(),
      instance.tenant(),
      instance.cluster_id(),
      instance.tenant_id(),
      convert_rps,
      BINLOG_INSTANCE_CONVERT_FETCH_RPS_TYPE);
  PrometheusExposer::mark_binlog_instance_metric(instance.instance_name(),
      instance.cluster(),
      instance.tenant(),
      instance.cluster_id(),
      instance.tenant_id(),
      convert_eps,
      BINLOG_INSTANCE_CONVERT_STORAGE_RPS_TYPE);

  PrometheusExposer::mark_binlog_dump_error_count_metric(g_cluster->get_node_info().id(),
      instance.instance_name(),
      instance.cluster(),
      instance.tenant(),
      instance.cluster_id(),
      instance.tenant_id(),
      BINLOG_INSTANCE_DUMP_ERROR_COUNT_TYPE,
      dump_error_count);
}

void MetricTask::binlog_instance_exploration(ClusterConfig* cluster_config,
    const InstanceFailoverCb& instance_failover_cb, BinlogEntry instance, const Node& node)
{
  BinlogEntry binlog_instance(instance);
  MySQLConnection connection;
  auto conn = connection.get_conn(cluster_config->database_ip(),
      cluster_config->database_port(),
      cluster_config->user(),
      cluster_config->password(),
      cluster_config->database_name());
  if (conn == nullptr) {
    OMS_ERROR("Failed to get the connection");
    return;
  }
  defer(conn->close());
  std::pair<InstanceGtidSeq, InstanceGtidSeq> boundary_gtid_seq;
  if (OMS_OK !=
      InstanceGtidSeqDAO::get_instance_boundary_gtid_seq(conn.get(), instance.instance_name(), boundary_gtid_seq)) {
    OMS_ERROR("Failed to obtain boundary gtid seq of instance: {}, error: {}", instance.instance_name());
    return;
  };

  InstanceClient client;
  MySQLResultSet rs;
  /*!
   * When the client is successfully initialized, the probing operation is performed. If the client initializes
   * abnormally, it is directly considered that the probing failed.
   */
  int ret = client.init(instance);
  if (OMS_OK == ret) {
    ret = client.report(rs, boundary_gtid_seq.second.commit_version_start());
  }
  if (ret == OMS_OK && !rs.rows.empty()) {
    mark_converter_metric(instance, rs);
    try {
      auto row = rs.rows.front();
      int64_t delay = std::atoll(row.fields()[1].c_str());
      uint64_t minimum_dump_point = std::atoll(row.fields()[6].c_str());
      binlog_instance.set_heartbeat(Timer::now_s());
      binlog_instance.set_delay(delay);
      binlog_instance.set_min_dump_checkpoint(minimum_dump_point);

      std::string incr_gtid_seq_str = row.fields()[7];
      uint64_t last_purged_gtid = std::atoll(row.fields()[8].c_str());
      uint64_t last_purged_checkpoint = 0;
      if (row.fields().size() > 9) {
        last_purged_checkpoint = std::atoll(row.fields()[9].c_str());
      }
      if (OMS_OK != update_instance_gtid_seq(conn.get(),
                        binlog_instance,
                        boundary_gtid_seq,
                        last_purged_gtid,
                        incr_gtid_seq_str,
                        last_purged_checkpoint)) {
        OMS_ERROR("Failed to update instance gtid seq of instance [{}], incr gtid seq: {}, last purged gtid: {}",
            instance.instance_name(),
            incr_gtid_seq_str,
            last_purged_gtid);
        return;
      }

      /*!
       * @brief When the binlog instance status is not in the I_GRAYSCALE and I_PAUSED states,
       * as long as we successfully detect the activation, the instance is considered to be running.
       */
      if (instance.state() != binlog::InstanceState::GRAYSCALE || instance.state() != binlog::InstanceState::PAUSED) {
        binlog_instance.set_state(binlog::InstanceState::RUNNING);
      }

      ret = BinlogInstanceDAO::update_instance(conn.get(), binlog_instance, instance);
      if (ret != OMS_OK) {
        OMS_ERROR("Failed to report binlog instance information:{}", instance.instance_name());
        return;
      }
    } catch (sql::SQLException& e) {
      OMS_ERROR("Failed to explore OBI: {},reason:{}", instance.instance_name(), e.what());
      return;
    }
  } else {
    OMS_ERROR("Failed to detect binlog instance: {}", instance.instance_name());
    if ((Timer::now_s() - instance.heartbeat()) > instance.config()->expiration() &&
        instance.state() == InstanceState::RUNNING) {
      bool is_local = strcmp(instance.node_id().c_str(), node.id().c_str()) == 0;
      if (is_local && kill(instance.pid(), 0) == 0) {
        OMS_WARN("The local binlog instance [{}] actually alive through kill 0 with pid [{}]",
            instance.instance_name(),
            instance.pid());
        binlog_instance.set_heartbeat(Timer::now_s());
        if (BinlogInstanceDAO::update_instance(conn.get(), binlog_instance, instance) != OMS_OK) {
          OMS_ERROR("Failed to report binlog instance: {}", instance.instance_name());
        }
      } else {
        binlog_instance.set_state(InstanceState::OFFLINE);
        if (BinlogInstanceDAO::update_instance(conn.get(), binlog_instance, instance) != OMS_OK) {
          OMS_ERROR("Failed to report binlog instance: {}", instance.instance_name());
          return;
        }

        PrometheusExposer::mark_binlog_counter_metric(instance.node_id(),
            instance.instance_name(),
            instance.cluster(),
            instance.tenant(),
            instance.cluster_id(),
            instance.tenant_id(),
            BINLOG_INSTANCE_DOWN_TYPE);

        instance_failover_cb(instance);
      }
    }
  }
  mark_dump_metric(instance, client, rs);
}

int MetricTask::fetch_downtime_instances(string node_id, std::vector<BinlogEntry>& binlog_instances)
{
  auto conn = _node.get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection");
    return true;
  }
  defer(_node.get_sql_connection().release_connection(conn));
  BinlogInstanceDAO::downtime_instance_query(conn, binlog_instances, node_id);
  return false;
}

void MetricTask::mark_binlog_instance_down(string node_id)
{
  std::vector<BinlogEntry> binlog_instances;
  if (fetch_downtime_instances(node_id, binlog_instances) != OMS_OK) {
    return;
  }
  for (auto instance : binlog_instances) {
    PrometheusExposer::mark_binlog_counter_metric(instance.node_id(),
        instance.instance_name(),
        instance.cluster(),
        instance.tenant(),
        instance.cluster_id(),
        instance.tenant_id(),
        BINLOG_INSTANCE_DOWN_TYPE);
  }
}
int MetricTask::binlog_instances_exploration_task()
{
  /*!
   * @brief The first step is to conduct OBI exploration,And update the heartbeat and status of OBI
   */
  std::vector<BinlogEntry*> binlog_instances;
  defer(logproxy::release_vector(binlog_instances));

  if (query_surviving_instances(_node.get_node_info().id(), binlog_instances) != OMS_OK) {
    OMS_ERROR("Failed to query the surviving instances of the current node: {}", _node.get_node_info().id());
    return OMS_FAILED;
  }

  for (auto* instance : binlog_instances) {
    mark_instance_resource(instance);
    _node.get_thread_executor().submit(binlog_instance_exploration,
        _node.get_cluster_config(),
        _node.get_instance_failover_cb(),
        *instance,
        _node.get_node_info());
  }
  PrometheusExposer::mark_binlog_gauge_metric(
      g_cluster->get_node_info().id(), "", "", "", {}, {}, binlog_instances.size(), BINLOG_INSTANCE_NUM_TYPE);

  mark_binlog_instance_down(_node.get_node_info().id());
  return OMS_OK;
}

void MetaDBPushPullTask::run()
{
  while (is_run()) {
    /*!
     * @brief The second step is to report the heartbeat of the node. If the current node status is abnormal in the
     * cluster, anti-entropy is required.
     */
    node_exploration();
    _timer.sleep(this->_node.get_cluster_config()->push_pull_interval_us());
  }
}

int PrimaryInstanceDAO::query_primary_instance(
    sql::Connection* conn, PrimaryInstance& primary, const PrimaryInstance& condition)
{
  try {
    std::string sql = "SELECT * FROM primary_instance ";
    std::string where_clause;
    primary_instance_where(condition, where_clause, true);
    sql += where_clause;
    OMS_DEBUG("Query primary instance:[{},{}],sql: {}", condition.cluster(), condition.tenant(), sql);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_primary_instance(condition, stmt, count, true);
    sql::ResultSet* res = execute_query_with_retry(stmt);
    if (res == nullptr) {
      return OMS_FAILED;
    }
    defer(delete res);
    OMS_DEBUG("Query the number of rows:{}", res->rowsCount());
    while (res->next()) {
      convert_to_primary_instance(res, primary);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to query all primary instance,reason:{}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int PrimaryInstanceDAO::add_primary_instance(sql::Connection* conn, const PrimaryInstance& primary)
{
  try {
    string sql = add_primary_instance_sql(primary);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_primary_instance(primary, stmt, count);
    stmt->executeUpdate();
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to add primary instance: {},reason:{}", primary.serialize_to_json(), e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int PrimaryInstanceDAO::update_primary_instance(
    sql::Connection* conn, const PrimaryInstance& primary, const PrimaryInstance& condition)
{
  try {
    string sql = update_primary_instance_sql(primary, condition);
    OMS_DEBUG("update_primary_instance sql:{}", sql);
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(sql));
    int count = 1;
    assign_primary_instance(primary, stmt, count);
    assign_primary_instance(condition, stmt, count);
    if (stmt->executeUpdate() != 1) {
      OMS_ERROR("Failed to update primary instance:{}", primary.serialize_to_json());
      return OMS_FAILED;
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to update primary instance: {},reason:{}", primary.serialize_to_json(), e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

string PrimaryInstanceDAO::add_primary_instance_sql(const PrimaryInstance& primary)
{
  string sql = "INSERT INTO primary_instance (";
  string values = " VALUES (";
  int count = 0;

  if (!primary.cluster().empty()) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "cluster";
    values += "?";
    count++;
  }

  if (!primary.tenant().empty()) {
    if (count > 0) {
      sql += ", ";
      values += ", ";
    }
    sql += "tenant";
    values += "?";
    count++;
  }

  if (count > 0) {
    sql += ", ";
    values += ", ";
  }
  sql += "master_instance";
  values += "?";
  count++;

  sql += ")";
  values += ")";

  sql += values;
  return sql;
}

void PrimaryInstanceDAO::assign_primary_instance(
    const PrimaryInstance& primary, shared_ptr<sql::PreparedStatement>& stmt, int& count, bool query)
{
  if (!primary.cluster().empty()) {
    stmt->setString(count, primary.cluster());
    count++;
    OMS_DEBUG("set cluster={}", primary.cluster());
  }

  if (!primary.tenant().empty()) {
    stmt->setString(count, primary.tenant());
    count++;
    OMS_DEBUG("set tenant={}", primary.tenant());
  }

  if (!primary.master_instance().empty() || !query) {
    stmt->setString(count, primary.master_instance());
    count++;
    OMS_DEBUG("set master_instance={}", primary.master_instance());
  }
}

string PrimaryInstanceDAO::update_primary_instance_sql(const PrimaryInstance& primary, const PrimaryInstance& condition)
{
  string sql = "UPDATE primary_instance SET ";
  int count = 0;

  if (!primary.cluster().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "cluster = ?";
    count++;
  }

  if (!primary.tenant().empty()) {
    if (count > 0) {
      sql += ", ";
    }
    sql += "tenant = ?";
    count++;
  }

  if (count > 0) {
    sql += ", ";
  }
  sql += "master_instance = ?";
  count++;

  std::string where_clause;
  primary_instance_where(condition, where_clause);
  sql += where_clause;
  return sql;
}

void PrimaryInstanceDAO::primary_instance_where(const PrimaryInstance& primary, string& sql, bool query)
{
  int count = 0;

  if (!primary.cluster().empty()) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "cluster = ?";
    count++;
  }

  if (!primary.tenant().empty()) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "tenant = ?";
    count++;
  }

  if (!primary.master_instance().empty() || !query) {
    if (count > 0) {
      sql += " AND ";
    }
    sql += "master_instance = ?";
    count++;
  }

  if (count > 0) {
    sql = " WHERE " + sql;
  }
}

void PrimaryInstanceDAO::convert_to_primary_instance(const sql::ResultSet* res, PrimaryInstance* primary)
{
  primary->set_cluster(res->getString("cluster").c_str());
  primary->set_tenant(res->getString("tenant").c_str());
  primary->set_master_instance(res->getString("master_instance").c_str());
}

void PrimaryInstanceDAO::convert_to_primary_instance(const sql::ResultSet* res, PrimaryInstance& primary)
{
  primary.set_cluster(res->getString("cluster").c_str());
  primary.set_tenant(res->getString("tenant").c_str());
  primary.set_master_instance(res->getString("master_instance").c_str());
}

int InstanceGtidSeqDAO::get_instance_boundary_gtid_seq(sql::Connection* conn, const std::string& instance_name,
    std::pair<InstanceGtidSeq, InstanceGtidSeq>& boundary_gtid_seq)
{
  // "commit_version_start" remains in increasing order
  std::string first_gtid_query_sql = "SELECT * FROM instances_gtid_seq WHERE instance = \"" + instance_name +
                                     "\" ORDER BY commit_version_start ASC LIMIT 1";
  if (OMS_OK != execute_at_most_one_query(conn, first_gtid_query_sql, boundary_gtid_seq.first, gtid_seq_rs_converter)) {
    OMS_ERROR("Failed to query the first gtid of instance: {}", instance_name);
    return OMS_FAILED;
  }

  std::string last_gtid_query_sql = "SELECT * FROM instances_gtid_seq WHERE instance = \"" + instance_name +
                                    "\" ORDER BY commit_version_start DESC LIMIT 1";
  if (OMS_OK != execute_at_most_one_query(conn, last_gtid_query_sql, boundary_gtid_seq.second, gtid_seq_rs_converter)) {
    OMS_ERROR("Failed to query the last gtid of instance: {}", instance_name);
    return OMS_FAILED;
  }

  OMS_DEBUG("Instance [{}] boundary gtid seq: [{}, {}]",
      instance_name,
      boundary_gtid_seq.first.serialize_to_json(),
      boundary_gtid_seq.second.serialize_to_json());
  return OMS_OK;
}

int InstanceGtidSeqDAO::add_gtid_seq_batch(sql::Connection* conn, const std::vector<InstanceGtidSeq>& gtid_seq_vec)
{
  if (gtid_seq_vec.empty()) {
    // todo split vec
    return OMS_OK;
  }

  std::string add_batch_sql = "INSERT INTO instances_gtid_seq(cluster, tenant, instance, commit_version_start, "
                              "xid_start, gtid_start, trxs_num) VALUES ";
  for (const auto& gtid_seq : gtid_seq_vec) {
    add_batch_sql += "('" + gtid_seq.cluster() + "', '" + gtid_seq.tenant() + "', '" + gtid_seq.instance_name() +
                     "', " + std::to_string(gtid_seq.commit_version_start()) + ", '" + gtid_seq.xid_start() + "', " +
                     std::to_string(gtid_seq.gtid_start()) + ", " + std::to_string(gtid_seq.trxs_num()) + "),";
  }
  add_batch_sql.pop_back();

  int32_t affected_rows = 0;
  if (OMS_OK != execute_update(conn, add_batch_sql, affected_rows)) {
    return OMS_FAILED;
  }
  OMS_DEBUG("Batch add instance gtid seq sql: {}, affected rows: {}", add_batch_sql, affected_rows);
  return OMS_OK;
}

int InstanceGtidSeqDAO::remove_instance_gtid_seq_before_gtid(
    sql::Connection* conn, const std::string& instance_name, uint64_t gtid)
{
  std::string remove_gtid_seq_sql = "DELETE FROM instances_gtid_seq WHERE instance = \"" + instance_name +
                                    "\" AND gtid_start <= " + std::to_string(gtid) + " LIMIT " +
                                    std::to_string(Config::instance().max_delete_rows.val());

  OMS_DEBUG("Begin to remove gtid before checkpoint, sql: {}", remove_gtid_seq_sql);
  int32_t affected_rows = 0;
  do {
    if (OMS_OK != execute_update(conn, remove_gtid_seq_sql, affected_rows)) {
      return OMS_FAILED;
    }
    OMS_INFO("Remove instance [{}] gtid seq before [{}], affected rows: {}", instance_name, gtid, affected_rows);
  } while (affected_rows > 0);
  return OMS_OK;
}

int InstanceGtidSeqDAO::remove_instance_gtid_seq_before_checkpoint(
    sql::Connection* conn, const string& instance_name, uint64_t checkpoint)
{
  std::string remove_gtid_seq_sql = "DELETE FROM instances_gtid_seq WHERE instance = \"" + instance_name +
                                    "\" AND commit_version_start < " + std::to_string(checkpoint) +
                                    " AND xid_start = '' LIMIT " +
                                    std::to_string(Config::instance().max_delete_rows.val());

  OMS_DEBUG("Begin to remove gtid before checkpoint, sql: {}", remove_gtid_seq_sql);
  int32_t affected_rows = 0;
  do {
    if (OMS_OK != execute_update(conn, remove_gtid_seq_sql, affected_rows)) {
      return OMS_FAILED;
    }
    OMS_INFO("Removed instance [{}] gtid seq before [{}], affected rows: {}", instance_name, checkpoint, affected_rows);
  } while (affected_rows > 0);
  return OMS_OK;
}

void InstanceGtidSeqDAO::convert_to_instance_gtid_seq(const sql::ResultSet* res, InstanceGtidSeq& gtid_seq)
{
  gtid_seq.set_cluster(res->getString("cluster").c_str());
  gtid_seq.set_tenant(res->getString("tenant").c_str());
  gtid_seq.set_instance_name(res->getString("instance").c_str());
  gtid_seq.set_commit_version_start(res->getUInt64("commit_version_start"));
  gtid_seq.set_xid_start(res->getString("xid_start").c_str());
  gtid_seq.set_gtid_start(res->getUInt64("gtid_start"));
  gtid_seq.set_trxs_num(res->getUInt64("trxs_num"));
}

void UserDAO::convert_to_user(const sql::ResultSet* res, User& user)
{
  user.set_username(res->getString("username").c_str());
  user.set_password_sha1(res->getString("password_sha1").c_str());
  user.set_password(res->getString("password").c_str());
  user.set_tag(res->getInt("tag"));
}

void MetaDBPushPullTask::node_exploration()
{
  node_report();
  node_probe();
}

int MetricTask::query_surviving_instances(const std::string& node_id, vector<BinlogEntry*>& binlog_instances)
{
  BinlogEntry condition;
  condition.set_node_id(node_id);
  condition.set_state(InstanceState::RUNNING);
  auto conn = _node.get_sql_connection().get_conn(true);
  if (conn == nullptr) {
    OMS_ERROR("Failed to get the connection");
    return OMS_FAILED;
  }
  defer(_node.get_sql_connection().release_connection(conn));
  int ret = BinlogInstanceDAO::query_instance_by_entry(conn, binlog_instances, condition);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to query node information [node_id: {},node_ip: {},node_port: {}] from cluster",
        _node.get_node_info().id(),
        _node.get_node_info().ip(),
        _node.get_node_info().port());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int MetricTask::remote_non_live_node_instance_probe()
{
  vector<Node*> nodes;
  defer(release_vector(nodes));
  set<State> states{OFFLINE};

  if (query_nodes_by_state(nodes, states) != OMS_OK) {
    OMS_ERROR("Failed to query nodes by state");
    return OMS_FAILED;
  }

  for (const auto* node : nodes) {
    vector<BinlogEntry*> binlog_instances;
    defer(release_vector(binlog_instances));
    OMS_INFO("The current node [{}] is not alive, and all OBIs under the current node are explored remotely",
        node->identifier());
    if (query_surviving_instances(node->id(), binlog_instances) != OMS_OK) {
      OMS_ERROR("Failed to obtain surviving instances on offline node:{}", node->identifier());
      continue;
    }
    for (const auto* instance : binlog_instances) {
      if (instance->state() == InstanceState::RUNNING) {
        _node.get_thread_executor().submit(binlog_instance_exploration,
            _node.get_cluster_config(),
            _node.get_instance_failover_cb(),
            *instance,
            _node.get_node_info());
      }
    }

    mark_binlog_instance_down(node->id());
  }
  return OMS_OK;
}

MetricTask::MetricTask(MetaDBProtocol& node) : _node(node)
{
  _interval_us = node.get_cluster_config()->metric_interval_us();
}

void MetricTask::run()
{
  while (is_run()) {
    binlog_instances_exploration_task();
    remote_non_live_node_instance_probe();
    _timer.sleep(this->_interval_us);
  }
}

int MetricTask::query_nodes_by_state(vector<Node*>& nodes, set<State>& states)
{
  auto conn = _node.get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get the connection");
    return OMS_FAILED;
  }
  defer(_node.get_sql_connection().release_connection(conn));
  return NodeDAO::query_nodes(conn, nodes, states);
}

void MetricTask::mark_instance_resource(const BinlogEntry* instance)
{
  std::unique_lock<std::mutex> lock(g_metric_mutex);
  OMS_DEBUG("Collect instance resource indicators into prometheus exposer");
  // Collect instance resource indicator information
  std::string node_id = _node.get_node_info().id();
  std::string instance_name = instance->instance_name();
  std::string cluster = instance->cluster();
  std::string tenant = instance->tenant();
  std::string cluster_id = instance->cluster_id();
  std::string tenant_id = instance->tenant_id();
  for (auto const& iterator : g_metric->process_group_metric()->metric_group().get_items()) {
    auto* p_process_metric = ((ProcessMetric*)iterator);
    if (instance->pid() == p_process_metric->pid()) {
      //=========================> cpu start <====================
      PrometheusExposer::mark_binlog_instance_resource(node_id,
          instance_name,
          cluster,
          tenant,
          cluster_id,
          tenant_id,
          p_process_metric->cpu_status()->cpu_count(),
          BINLOG_INSTANCE_CPU_COUNT_TYPE);

      PrometheusExposer::mark_binlog_instance_resource(node_id,
          instance_name,
          cluster,
          tenant,
          cluster_id,
          tenant_id,
          p_process_metric->cpu_status()->cpu_used_ratio(),
          BINLOG_INSTANCE_CPU_USED_RATIO_TYPE);
      //=========================> cpu end <====================

      //=========================> mem start <====================
      PrometheusExposer::mark_binlog_instance_resource(node_id,
          instance_name,
          cluster,
          tenant,
          cluster_id,
          tenant_id,
          p_process_metric->memory_status()->mem_total_size_mb(),
          BINLOG_INSTANCE_MEM_TOTAL_SIZE_MB_TYPE);

      PrometheusExposer::mark_binlog_instance_resource(node_id,
          instance_name,
          cluster,
          tenant,
          cluster_id,
          tenant_id,
          p_process_metric->memory_status()->mem_used_size_mb(),
          BINLOG_INSTANCE_MEM_USED_SIZE_MB_TYPE);

      PrometheusExposer::mark_binlog_instance_resource(node_id,
          instance_name,
          cluster,
          tenant,
          cluster_id,
          tenant_id,
          p_process_metric->memory_status()->mem_used_ratio(),
          BINLOG_INSTANCE_MEM_USED_RATIO_TYPE);
      //=========================> mem end <====================

      //=========================> disk start <====================
      PrometheusExposer::mark_binlog_instance_resource(node_id,
          instance_name,
          cluster,
          tenant,
          cluster_id,
          tenant_id,
          p_process_metric->disk_status()->disk_usage_size_process_mb(),
          BINLOG_INSTANCE_DISK_TOTAL_SIZE_MB_TYPE);
      PrometheusExposer::mark_binlog_instance_resource(node_id,
          instance_name,
          cluster,
          tenant,
          cluster_id,
          tenant_id,
          p_process_metric->disk_status()->disk_used_ratio(),
          BINLOG_INSTANCE_DISK_USED_RATIO_TYPE);
      //=========================> disk start <====================

      //=========================> network start <====================
      PrometheusExposer::mark_binlog_instance_resource(node_id,
          instance_name,
          cluster,
          tenant,
          cluster_id,
          tenant_id,
          p_process_metric->network_status()->network_rx_bytes(),
          BINLOG_INSTANCE_NETWORK_RX_BYTES_TYPE);
      PrometheusExposer::mark_binlog_instance_resource(node_id,
          instance_name,
          cluster,
          tenant,
          cluster_id,
          tenant_id,
          p_process_metric->network_status()->network_wx_bytes(),
          BINLOG_INSTANCE_NETWORK_WX_BYTES_TYPE);

      //=========================> network end <====================

      PrometheusExposer::mark_binlog_instance_resource(node_id,
          instance_name,
          cluster,
          tenant,
          cluster_id,
          tenant_id,
          p_process_metric->fd_count(),
          BINLOG_INSTANCE_FD_COUNT_TYPE);
    }
  }
}

int MetaDBPushPullTask::query_nodes_by_state(vector<Node*>& nodes, set<State>& states)
{
  auto conn = _node.get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get the connection");
    return OMS_FAILED;
  }
  defer(_node.get_sql_connection().release_connection(conn));
  return NodeDAO::query_nodes(conn, nodes, states);
}

int MetaDBPushPullTask::offline_node(Node& node)
{
  auto conn = _node.get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get the connection");
    return OMS_FAILED;
  }
  defer(_node.get_sql_connection().release_connection(conn));
  node.set_state(OFFLINE);
  node.set_last_modify(Timer::now_s());
  int ret = NodeDAO::update_node(conn, node);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to update node information [node_id: {},node_ip: {},node_port: {}] to cluster",
        _node.get_node_info().id(),
        _node.get_node_info().ip(),
        _node.get_node_info().port());
    return OMS_FAILED;
  }
  PrometheusExposer::mark_binlog_counter_metric(node.ip(), "", "", "", {}, {}, BINLOG_MANAGER_DOWN_COUNT_TYPE);
  OMS_INFO("Node [{}] has not reported for a long time, and is updated to offline", node.id());
  return OMS_OK;
}
int MetaDBPushPullTask::node_probe()
{
  vector<Node*> nodes;
  defer(release_vector(nodes));
  set<State> states{ONLINE, SUSPECT};
  if (query_nodes_by_state(nodes, states) != OMS_OK) {
    OMS_ERROR("Failed to query nodes by state");
    return OMS_FAILED;
  }

  for (const auto* node : nodes) {
    Node newest_node;
    newest_node.set_id(node->id());
    if (query_node(newest_node) != OMS_OK) {
      OMS_ERROR("Failed to get the latest status of the node:{}", newest_node.identifier());
      continue;
    }
    if (strcmp(newest_node.id().c_str(), _node.get_node_info().id().c_str()) != 0 &&
        (Timer::now_s() - newest_node.last_modify()) > newest_node.node_config()->expiration()) {
      if (offline_node(newest_node) != OMS_OK) {
        OMS_ERROR("Offline node failed, node_id: {}", newest_node.identifier());
        continue;
      }
    }
  }

  return OMS_OK;
}

int MetaDBPushPullTask::query_node(Node& node)
{
  auto conn = _node.get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get the connection");
    return OMS_FAILED;
  }
  defer(_node.get_sql_connection().release_connection(conn));
  return NodeDAO::query_node(conn, node);
}

int MetaDBPushPullTask::register_node()
{
  auto conn = _node.get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get the connection");
    return OMS_FAILED;
  }
  defer(_node.get_sql_connection().release_connection(conn));
  return NodeDAO::insert(conn, _node.get_node_info());
}
int MetaDBPushPullTask::node_report(Node node)
{
  uint32_t node_state = UNDEFINED;
  if (node.state() != GRAYSCALE && node.state() != PAUSED) {
    node_state = ONLINE;
  }
  node.set_last_modify(Timer::now_s());
  node.set_state(node_state);
  {
    std::lock_guard<std::mutex> guard(g_metric_mutex);
    node.metric()->assign(g_metric);
  }
  auto conn = _node.get_sql_connection().get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get the connection");
    return OMS_FAILED;
  }
  defer(_node.get_sql_connection().release_connection(conn));
  return NodeDAO::update_node(conn, node);
}
int MetaDBPushPullTask::node_report()
{
  Node node;
  node.set_id(_node.get_node_info().id());
  int ret = query_node(node);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to pull the node information [node_id: {},node_ip: {},node_port: {}] from the cluster",
        _node.get_node_info().id(),
        _node.get_node_info().ip(),
        _node.get_node_info().port());

    return OMS_FAILED;
  }
  /*!
   * @brief The query result is empty, indicating that the node information does not exist in the cluster and needs to
   * be newly registered.
   */
  if (node.state() == UNDEFINED) {
    ret = register_node();
    if (ret != OMS_OK) {
      OMS_ERROR("Failed to register node [node_id: {},node_ip: {},node_port: {}] to cluster",
          _node.get_node_info().id(),
          _node.get_node_info().ip(),
          _node.get_node_info().port());
      return OMS_FAILED;
    }
  } else {
    ret = node_report(node);
    if (ret != OMS_OK) {
      OMS_ERROR("Failed to update node information [node_id: {},node_ip: {},node_port: {}] to cluster",
          _node.get_node_info().id(),
          _node.get_node_info().ip(),
          _node.get_node_info().port());
      return OMS_FAILED;
    }
  }
  return OMS_OK;
}

MetaDBPushPullTask::MetaDBPushPullTask(MetaDBProtocol& node) : _node(node)
{
  _push_pull_interval_us = node.get_cluster_config()->push_pull_interval_us();
}
}  // namespace oceanbase::logproxy
// namespace oceanbase::logproxy
