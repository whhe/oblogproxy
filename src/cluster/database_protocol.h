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
#include <map>
#include <shared_mutex>
#include <vector>
#include "metric/sys_metric.h"
#include "cluster/transport.h"
#include "codec/message.h"
#include "cluster/node.h"
#include "task.h"
#include "common.h"
#include "cluster_protocol.h"
#include "mysql_connecton_wrapper.h"
#include "thread_pool_executor.h"
#include "binlog/instance_client.h"
#include "metric/status_thread.h"
#include "str.h"

namespace oceanbase::logproxy {

class MetaDBProtocol;

class MetaDBPushPullTask final : public Thread {
public:
  explicit MetaDBPushPullTask(MetaDBProtocol& node);

  void node_exploration();

  int node_report();

  int query_nodes_by_state(vector<Node*>& nodes, set<State>& states);

  int offline_node(Node& node);

  int node_probe();

  int query_node(Node& node);

  int register_node();

  int node_report(Node node);

private:
  void run() override;

  uint64_t _push_pull_interval_us;
  MetaDBProtocol& _node;
  Timer _timer;
};

class MetricTask final : public Thread {
public:
  explicit MetricTask(MetaDBProtocol& node);

  void mark_instance_resource(const BinlogEntry* instance);

  int binlog_instances_exploration_task();

  static void binlog_instance_exploration(ClusterConfig* cluster_config, const InstanceFailoverCb& instance_failover_cb,
      BinlogEntry instance, const Node& node);

  int fetch_downtime_instances(string node_id, std::vector<BinlogEntry>& binlog_instances);

  void mark_binlog_instance_down(string node_id);

  int query_surviving_instances(const std::string& node_id, vector<BinlogEntry*>& binlog_instances);

  int remote_non_live_node_instance_probe();

  int query_nodes_by_state(vector<Node*>& nodes, set<State>& states);

private:
  void run() override;

  uint64_t _interval_us;
  MetaDBProtocol& _node;
  Timer _timer;
};

class MetaDBProtocol final : public ClusterProtocol {
public:
  MetaDBProtocol(ClusterConfig* cluster_config, TaskExecutorCb& task_executor_cb, InstanceFailoverCb& failover_cb);

  ~MetaDBProtocol() override;

  int register_node() override;

  int unregister_node() override;

  int publish_task(Task& task) override;

  int add_instance(const BinlogEntry& instance, Task& task) override;

  int drop_instances(std::vector<Task>& task_vec) override;

  int fetch_task(const Task& task, std::vector<Task*>& tasks) override;

  int fetch_unfinished_tasks(const string& node_id, vector<Task>& tasks) override;

  int fetch_recent_offline_task(const string& instance_name, Task& task) override;

  int query_all_nodes(std::vector<Node*>& nodes) override;

  int query_nodes(std::vector<Node*>& nodes, const Node& node) override;

  int query_nodes(set<std::string>& node_ids, map<std::string, Node>& nodes) override;

  int query_instances(std::vector<BinlogEntry*>& instances, const BinlogEntry& condition) override;

  int query_nodes(vector<Node*>& nodes, std::set<State> node_states) override;

  int query_node_by_id(const std::string& node_id, Node& node) override;

  int query_instance_by_name(const std::string& instance_name, BinlogEntry& entry) override;

  int query_master_instance(const std::string& cluster, const std::string& tenant, std::string& instance_name) override;

  int determine_primary_instance(const std::string& cluster, const std::string& tenant,
      const std::string& instance_name, std::string& master) override;

  int reset_master_instance(
      const std::string& cluster, const std::string& tenant, const std::string& instance_name) override;

  int query_tenant_surviving_instances(
      std::string& cluster, std::string& tenant, std::vector<BinlogEntry>& instances) override;

  int query_instances(std::vector<BinlogEntry*>& instances, std::string slot, SlotType type) override;

  int query_instances(vector<BinlogEntry*>& instances, const set<std::string>& instance_names) override;

  int query_instances(vector<BinlogEntry*>& instances, const set<InstanceState>& instance_states) override;

  int query_all_instances(vector<BinlogEntry*>& instances) override;

  int update_instances(const BinlogEntry& instance, const BinlogEntry& condition) override;

  int add_instance(const BinlogEntry& instance) override;

  int query_task_by_id(const std::string& task_id, Task& task) override;

  int update_task(Task& task) override;

  int update_tasks(Task& task, const Task& condition) override;

  int update_instance(const BinlogEntry& instance) override;

  void query_config_item_by_granularity(ConfigTemplate& config, sql::Connection* conn, const ConfigTemplate& condition);

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

  int query_user_by_name(const std::string& name, User& user) override;

  int alter_user_password(const User& user, const std::string& new_password) override;

  int query_sys_user(User& user) override;

  /*!
   * Connection pool for handling OBI related requests
   * @return
   */
  MySQLConnection& get_sql_connection();

private:
  MySQLConnection _sql_connection;
  MetaDBPushPullTask _push_pull_task{*this};
  MetricTask _metric_task{*this};
};

class NodeDAO {
  OMS_SINGLETON(NodeDAO);

public:
  /*!
   * @brief Query all Nodes in the cluster from the database
   * @param conn
   * @param nodes
   * @return
   */
  static int query_all_nodes(sql::Connection* conn, std::vector<Node*>& nodes);

  /*!
   * @brief Query nodes that meet the conditions based on the specified {node} condition
   * @param conn
   * @param nodes
   * @param node
   * @return
   */
  static int query_nodes(sql::Connection* conn, std::vector<Node*>& nodes, const Node& node);

  static int query_node(sql::Connection* conn, Node& node);

  /*!
   * @brief Query nodes that meet the conditions based on the specified {node} condition
   * @param conn
   * @param nodes
   * @param state_conditions
   * @return
   */
  static int query_nodes(sql::Connection* conn, std::vector<Node*>& nodes, const std::set<State>& state_conditions);

  static int query_nodes(sql::Connection* conn, set<std::string>& node_ids, map<std::string, Node>& nodes);

  /*!
   * @brief Query the corresponding indicator information based on the specified {node}
   * @param conn
   * @param metric
   * @param node
   * @return
   */
  static int query_metric(sql::Connection* conn, SysMetric& metric, const Node& node);

  /*!
   * @brief Insert a {node} record
   * @param conn
   * @param node
   * @return
   */
  static int insert(sql::Connection* conn, const Node& node);

  /*!
   * @brief Update the specified {node} record
   * @param conn
   * @param node
   * @param condition
   * @return
   */
  static int update_node(sql::Connection* conn, const Node& node, const Node& condition);

  /*!
   * @brief Update the specified {node} record
   * @param conn
   * @param node
   * @return
   */
  static int update_node(sql::Connection* conn, const Node& node);

  /*!
   * @brief delete all records satisfying node
   * @param conn
   * @param node
   * @return
   */
  static int delete_nodes(sql::Connection* conn, const Node& node);

  static void convert_to_node(const sql::ResultSet* res, Node& node);

  static string nodes_where_clause_sql(const Node& node);

  static void assgin_nodes(const Node& node, shared_ptr<sql::PreparedStatement>& stmt, int& count);

  static std::string add_node_sql(const Node& node);

  static void nodes_set_clause(const Node& node, string& sql);
};
static std::function<void(const sql::ResultSet*, Node&)> node_rs_converter = NodeDAO::convert_to_node;

class BinlogInstanceDAO {
  OMS_SINGLETON(BinlogInstanceDAO);

public:
  static int query_all_instances(sql::Connection* conn, std::vector<BinlogEntry*>& binlog_instances);

  static int query_tenant_surviving_instances(
      sql::Connection* conn, std::string& cluster, std::string& tenant, std::vector<BinlogEntry>& instances);

  /*!
   * @brief Query binlog instances that meet the conditions based on the incoming {condition}
   * @param conn
   * @param binlog_instances
   * @param condition
   * @return
   */
  static int query_instance_by_entry(
      sql::Connection* conn, std::vector<BinlogEntry*>& binlog_instances, const BinlogEntry& condition);

  static int downtime_instance_query(
      sql::Connection* conn, std::vector<BinlogEntry>& binlog_instances, std::string node_id);

  static int query_instance_by_row_lock(
      sql::Connection* conn, std::vector<BinlogEntry*>& binlog_instances, const BinlogEntry& condition);

  static int query_instances(
      sql::Connection* conn, vector<BinlogEntry*>& instances, const set<std::string>& instance_names);

  static int query_instance(sql::Connection* conn, BinlogEntry& instance, const std::string& instance_name);

  static int query_instances(
      sql::Connection* conn, vector<BinlogEntry*>& instances, const set<InstanceState>& instance_states);

  static int query_instances_by_node(sql::Connection* conn, vector<BinlogEntry*>& instances, const Node& node);

  static int add_instances(sql::Connection* conn, std::vector<BinlogEntry*>& binlog_instances);

  static int add_instance(sql::Connection* conn, const BinlogEntry& binlog_instance);

  static int update_instance(sql::Connection* conn, const BinlogEntry& binlog_instance, const BinlogEntry& condition);

  static int update_instance(
      sql::Connection* conn, const BinlogEntry& binlog_instance, const std::string& instance_name);

  static int update_instance_state(sql::Connection* conn, const std::string&, InstanceState);

  static int delete_instances(sql::Connection* conn, std::vector<BinlogEntry*>& binlog_instances);

  static void convert_to_instance(const sql::ResultSet* res, BinlogEntry& instance);

  static string splice_add_instance_sql(const BinlogEntry& binlog_instance);

  static void assign_instance(const BinlogEntry& binlog_instance, shared_ptr<sql::PreparedStatement>& stmt, int& count,
      bool where = false, bool must_set_min_dump_checkpoint = false);

  static string splice_update_instance_sql(const BinlogEntry& binlog_instance, const BinlogEntry& condition);

  static void instance_where_clause_sql(const BinlogEntry& condition, string& where_clause);
};
static std::function<void(const sql::ResultSet*, BinlogEntry&)> instance_rs_converter =
    BinlogInstanceDAO::convert_to_instance;

class TaskDAO {
  OMS_SINGLETON(TaskDAO);

public:
  static int query_all_tasks(sql::Connection* conn, std::vector<Task*>& tasks);

  static int query_tasks_by_entry(sql::Connection* conn, std::vector<Task*>& tasks, const Task& condition);

  static int query_task_by_entry(sql::Connection* conn, Task& task, const Task& condition);

  static int add_tasks(sql::Connection* conn, const std::vector<Task>& tasks);

  static int add_task(sql::Connection* conn, Task& task);

  static int update_task(sql::Connection* conn, Task& task, const Task& condition);

  static int delete_task(sql::Connection* conn, const Task& task);

  static int delete_tasks(sql::Connection* conn, std::vector<Task>& tasks);

  static string add_task_sql(const Task& task);

  static void assign_task(const Task& task, shared_ptr<sql::PreparedStatement>& stmt, int& count);

  static string update_task_sql(const Task& task, const Task& condition);

  static void task_where(const Task& task, string& sql);

  static void convert_to_task(const sql::ResultSet* res, Task& task);
};
static std::function<void(const sql::ResultSet*, Task&)> task_rs_converter = TaskDAO::convert_to_task;

class ConfigTemplateDAO {
  OMS_SINGLETON(ConfigTemplateDAO);

public:
  static int query_config_item(sql::Connection* conn, ConfigTemplate& config, const ConfigTemplate& condition);

  static int query_config_items(
      sql::Connection* conn, std::vector<ConfigTemplate*>& configs, const ConfigTemplate& condition);

  static int query_config_items(
      sql::Connection* conn, std::map<std::string, std::string>& configs, const ConfigTemplate& condition);

  static int update_config_item(sql::Connection* conn, ConfigTemplate& config, const ConfigTemplate& condition);

  static int add_config_item(sql::Connection* conn, ConfigTemplate& config, const ConfigTemplate& condition);

  static string add_config_item_sql(const ConfigTemplate& config);

  static void assign_config_item(
      const ConfigTemplate& config, shared_ptr<sql::PreparedStatement>& stmt, int& count, bool query = false);

  static string update_config_item_sql(const ConfigTemplate& config, const ConfigTemplate& condition);

  static void config_item_where(const ConfigTemplate& config, string& sql, bool query = false);

  static void convert_to_config_item(const sql::ResultSet* res, ConfigTemplate* config);

  static void convert_to_config_item(const sql::ResultSet* res, ConfigTemplate& config);

  static int replace_into_config_item(sql::Connection* conn, ConfigTemplate& config);

  static string replace_into_config_item_sql(const ConfigTemplate& config);

  static int delete_config_item(sql::Connection* conn, ConfigTemplate& config);
};

class PrimaryInstanceDAO {
  OMS_SINGLETON(PrimaryInstanceDAO);

public:
  static int query_primary_instance(sql::Connection* conn, PrimaryInstance& primary, const PrimaryInstance& condition);

  static int add_primary_instance(sql::Connection* conn, const PrimaryInstance& primary);

  static int update_primary_instance(
      sql::Connection* conn, const PrimaryInstance& primary, const PrimaryInstance& condition);

  static string add_primary_instance_sql(const PrimaryInstance& primary);

  static void assign_primary_instance(
      const PrimaryInstance& primary, shared_ptr<sql::PreparedStatement>& stmt, int& count, bool query = false);

  static string update_primary_instance_sql(const PrimaryInstance& primary, const PrimaryInstance& condition);

  static void primary_instance_where(const PrimaryInstance& primary, string& sql, bool query = false);

  static void convert_to_primary_instance(const sql::ResultSet* res, PrimaryInstance* primary);

  static void convert_to_primary_instance(const sql::ResultSet* res, PrimaryInstance& primary);
};

class InstanceGtidSeqDAO {
  OMS_SINGLETON(InstanceGtidSeqDAO);

public:
  static int get_instance_boundary_gtid_seq(sql::Connection* conn, const std::string& instance_name,
      std::pair<InstanceGtidSeq, InstanceGtidSeq>& boundary_gtid_seq);

  static int add_gtid_seq_batch(sql::Connection* conn, const std::vector<InstanceGtidSeq>& gtid_seq_vec);

  static int remove_instance_gtid_seq_before_gtid(
      sql::Connection* conn, const std::string& instance_name, uint64_t gtid);

  static int remove_instance_gtid_seq_before_checkpoint(
      sql::Connection* conn, const std::string& instance_name, uint64_t checkpoint);

  static void convert_to_instance_gtid_seq(const sql::ResultSet* res, InstanceGtidSeq& gtid_seq);
};
static std::function<void(const sql::ResultSet*, InstanceGtidSeq&)> gtid_seq_rs_converter =
    InstanceGtidSeqDAO::convert_to_instance_gtid_seq;

class UserDAO {
public:
  static void convert_to_user(const sql::ResultSet* res, User& user);
};
static std::function<void(const sql::ResultSet*, User&)> user_rs_converter = UserDAO::convert_to_user;

}  // namespace oceanbase::logproxy
