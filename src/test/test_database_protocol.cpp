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

#include "gtest/gtest.h"
#include "database_protocol.h"
#include "common_util.h"

namespace oceanbase {
namespace logproxy {
TEST(TaskDAO, add_task)
{
  MySQLConnection connection;
  // TODO 要注释掉连接信息,测试中先在单测中保留
  int ret = connection.init("11.124.9.19", 10011, "logproxy@mysql#ob4x.admin", "123456", "binlog_cluster");
  if (ret != OMS_OK) {
    return;
  }

  Task task;
  task.set_instance_name("test");
  task.set_execution_node(binlog::CommonUtils::generate_trace_id());
  task.set_status(WAITING);
  auto* param = new CreateInstanceTaskParam();
  param->set_cluster("cluster_test");
  param->set_tenant("tenant_test");
  task.set_task_param(param);
  TaskDAO::instance().add_task(connection.get_conn(), task);

  Task query;
  Task condition;
  condition.set_task_id(task.task_id());
  OMS_INFO("condition:{}", condition.serialize_to_json());
  TaskDAO::instance().query_task_by_entry(connection.get_conn(), query, condition);
  OMS_INFO(query.serialize_to_json());
  ASSERT_STREQ(query.instance_name().c_str(), task.instance_name().c_str());

  query.set_last_modify(Timer::now_s());
  query.set_instance_name("test2");
  TaskDAO::instance().update_task(connection.get_conn(), query, condition);

  TaskDAO::instance().query_task_by_entry(connection.get_conn(), query, condition);

  OMS_INFO(query.serialize_to_json());
  ASSERT_STREQ(query.instance_name().c_str(), "test2");
}

TEST(BinlogInstanceDAO, add_instance)
{
  MySQLConnection connection;
  // TODO 要注释掉连接信息,测试中先在单测中保留
  int ret = connection.init("11.124.9.19", 10011, "logproxy@mysql#ob4x.admin", "123456", "binlog_cluster");
  if (ret != OMS_OK) {
    return;
  }
  BinlogEntry binlog_entry;
  binlog_entry.set_instance_name("test2");
  binlog_entry.set_cluster("cluster");
  binlog_entry.set_work_path("/home/ds/oblogproxy/run/cluster/tenant/test2");
  BinlogInstanceDAO::instance().add_instance(connection.get_conn(), binlog_entry);
}

TEST(model, release)
{
  Task create_instance_task;
  create_instance_task.set_execution_node("xxx");
  create_instance_task.set_type(TaskType::CREATE_INSTANCE);
  create_instance_task.set_status(TaskStatus::WAITING);
  create_instance_task.set_instance_name("test");
  auto* param = new CreateInstanceTaskParam();
  param->set_cluster("cluster");
  param->set_tenant("tenant");
  create_instance_task.set_task_param(param);
}

TEST(PrimaryInstance, query_master_instance)
{
  MySQLConnection connection;
  // TODO 要注释掉连接信息,测试中先在单测中保留
  int ret = connection.init("11.124.9.19", 10011, "logproxy@mysql#ob4x.admin", "123456", "binlog_cluster");
  if (ret != OMS_OK) {
    return;
  }
  PrimaryInstance binlog_entry;
  binlog_entry.set_cluster("cluster_test");
  binlog_entry.set_tenant("tenant_test");
  binlog_entry.set_master_instance("cluster_test#tenant_test$1");
  PrimaryInstanceDAO::add_primary_instance(connection.get_conn(), binlog_entry);
  PrimaryInstance primary_instance;
  PrimaryInstance condition;
  condition.set_cluster("cluster_test");
  condition.set_tenant("tenant_test");
  PrimaryInstanceDAO::query_primary_instance(connection.get_conn(), primary_instance, condition);
  OMS_INFO("primary instance:{}", primary_instance.serialize_to_json());
  ASSERT_EQ("cluster_test#tenant_test$1", primary_instance.master_instance());
}

TEST(MetaDBProtocol, master_instance)
{
  auto* config = new ClusterConfig();
  config->set_database_ip("11.124.9.19");
  config->set_database_name("binlog_cluster");
  config->set_node_id(CommonUtils::generate_trace_id());
  config->set_database_port(10011);
  config->set_password("123456");
  config->set_user("logproxy@mysql#ob4x.admin");
  TaskExecutorCb cb;
  InstanceFailoverCb instance_recover_cb;
  MetaDBProtocol protocol = MetaDBProtocol(config, cb, instance_recover_cb);

  std::string cluster = "cluster_test";
  std::string tenant = "tenant_test";
  std::string master = "";
  protocol.query_master_instance(cluster, tenant, master);
  OMS_INFO("master:{}", master);
}

TEST(MetaDBProtocol, determine_primary_instance)
{
  auto* config = new ClusterConfig();
  config->set_database_ip("11.124.9.19");
  config->set_database_name("binlog_cluster");
  config->set_node_id(CommonUtils::generate_trace_id());
  config->set_database_port(10011);
  config->set_password("123456");
  config->set_user("logproxy@mysql#ob4x.admin");
  TaskExecutorCb cb;
  InstanceFailoverCb instance_recover_cb;
  MetaDBProtocol protocol = MetaDBProtocol(config, cb, instance_recover_cb);

  std::string cluster = "cluster_test";
  std::string tenant = "tenant_test";
  std::string master = "cluster_test#tenant_test$1" + CommonUtils::generate_trace_id();
  std::string existed_master;
  protocol.determine_primary_instance(cluster, tenant, master, existed_master);
  OMS_INFO("master:{},existed_master:{}", master, existed_master);
  ASSERT_EQ("cluster_test#tenant_test$1", existed_master);

  existed_master = "";
  protocol.reset_master_instance(cluster, tenant, "cluster_test#tenant_test$1");
  protocol.determine_primary_instance(cluster, tenant, master, existed_master);
  OMS_INFO("master:{},existed_master:{}", master, existed_master);
  ASSERT_EQ("", existed_master);
}

TEST(MetaDBProtocol, query_instance_configs)
{
  auto* config = new ClusterConfig();
  config->set_database_ip("11.124.9.19");
  config->set_database_name("binlog_cluster");
  config->set_node_id(CommonUtils::generate_trace_id());
  config->set_database_port(10011);
  config->set_password("123456");
  config->set_user("logproxy@mysql#ob4x.admin");
  TaskExecutorCb cb;
  InstanceFailoverCb instance_recover_cb;
  MetaDBProtocol protocol = MetaDBProtocol(config, cb, instance_recover_cb);
  /*
   * case 1 Test global configuration acquisition
   */
  {
    map<string, string> configs;
    protocol.query_instance_configs(configs, "", "", "", "");
    for (auto const& config : configs) {
      OMS_INFO("[{}={}]", config.first, config.second);
    }
  }

  /*
   * case 2 Test the global configuration acquisition of the specified key
   */
  {
    ConfigTemplate config;
    protocol.query_init_instance_config(config, "max_binlog_size", "", "", "", "");
    OMS_INFO("[{}={}]", config.key_name(), config.value());
    ASSERT_STREQ("max_binlog_size", config.key_name().c_str());
    ASSERT_STREQ("536870912", config.value().c_str());
  }
}

TEST(MetaDBProtocol, replace_config)
{
  auto* config = new ClusterConfig();
  config->set_database_ip("11.124.9.19");
  config->set_database_name("binlog_cluster");
  config->set_node_id(CommonUtils::generate_trace_id());
  config->set_database_port(10011);
  config->set_password("123456");
  config->set_user("logproxy@mysql#ob4x.admin");
  TaskExecutorCb cb;
  InstanceFailoverCb instance_recover_cb;
  MetaDBProtocol protocol = MetaDBProtocol(config, cb, instance_recover_cb);
  /*
   * case 1 replace global level parameter
   */
  {
    std::string key = "binlog_max_event_buffer_bytes";
    std::string value = "10000000000000";
    auto scope = "";
    protocol.replace_config(key, value, G_GLOBAL, scope);
    ConfigTemplate config;
    protocol.query_init_instance_config(config, key, "", "", "", "");
    ASSERT_STREQ(value.c_str(), config.value().c_str());
  }

  /*
   * case 2 replace group level parameter
   */
  {
    std::string key = "binlog_max_event_buffer_bytes";
    std::string value = "10000000000000";
    auto scope = "group1";
    protocol.replace_config(key, value, G_GROUP, scope);
    ConfigTemplate config;
    protocol.query_init_instance_config(config, key, "group1", "", "", "");
    ASSERT_STREQ(value.c_str(), config.value().c_str());
  }

  /*
   * case 3 delete group level parameter
   */
  {
    std::string key = "binlog_max_event_buffer_bytes";
    std::string value = "10000000000000";
    auto scope = "group1";
    protocol.delete_config(key, G_GROUP, scope);
    ConfigTemplate config;
    protocol.query_init_instance_config(config, key, "group1", "", "", "");
    ASSERT_STREQ(value.c_str(), config.value().c_str());
    ASSERT_EQ(config.granularity(), 0);
  }
}

}  // namespace logproxy
}  // namespace oceanbase
