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
#include "common.h"
#include "log.h"
#include "mysql_connecton_wrapper.h"
#include "guard.hpp"
#include "database_protocol.h"

using namespace oceanbase::logproxy;

TEST(MySQLConnection, connect)
{
  MySQLConnection my_sql_connection;
  std::string ip = "127.0.0.1";
  int port = 10011;
  std::string user = "";
  std::string password = "";
  std::string database = "";
  my_sql_connection.init(ip, port, user, password, database);
  std::unique_ptr<sql::PreparedStatement> stmt(my_sql_connection.get_conn()->prepareStatement("show tables"));
  sql::ResultSet* res = stmt->executeQuery();
  defer(delete res);
  while (res->next()) {
    OMS_INFO(res->getString(1));
  }
}

TEST(ClusterDAO, add_node)
{
  Node node;
  node.set_ip("127.0.0.1");
  node.set_port(2983);
  std::string sql = NodeDAO::instance().add_node_sql(node);
  OMS_INFO(sql);
  ASSERT_STREQ("INSERT INTO nodes (ip,port) VALUES (?,?)", sql.c_str());
}

TEST(ClusterDAO, add_instance)
{
  BinlogEntry instance;
  instance.set_ip("127.0.0.1");
  instance.set_port(2983);
  std::string sql = BinlogInstanceDAO::instance().splice_add_instance_sql(instance);
  OMS_INFO(sql);
  ASSERT_STREQ("INSERT INTO binlog_instances (ip,port) VALUES (?,?)", sql.c_str());
}

TEST(ClusterDAO, update_instance)
{
  BinlogEntry instance;
  instance.set_ip("127.0.0.1");
  instance.set_port(2983);
  BinlogEntry condition;
  condition.set_state(InstanceState::FAILED);
  std::string update_instance_sql = BinlogInstanceDAO::instance().splice_update_instance_sql(instance, condition);
  OMS_INFO(update_instance_sql);
  ASSERT_STREQ("UPDATE binlog_instances SET ip = ?, port = ? WHERE state = ?", update_instance_sql.c_str());
}

TEST(ClusterDAO, uds)
{
  MySQLConnection connection;
  std::string _socket_path = "/data/huaqing/package/oms-logproxy/packenv/oblogproxy/run/binlogit/ob_mysql/"
                             "binlogit$ob_mysql$3/binlog_instance.socket";
  connection.init(_socket_path, "", "");
  try {
    std::shared_ptr<sql::Connection> conn(connection.get_conn(false));
    if (conn == nullptr) {
      OMS_ERROR("Failed to initialize OBI :{} connection", _socket_path);
    }
    std::shared_ptr<sql::PreparedStatement> statement(conn->prepareStatement("report"));
    if (statement != nullptr) {
      auto* rs = statement->executeQuery();
      while (rs->next()) {
        int64_t delay = rs->getInt64("convert_delay");
        uint64_t minimum_dump_point = rs->getUInt64("min_dump_checkpoint");
        OMS_INFO("delay:{},checkpoint:{}", delay, minimum_dump_point);
      }
    } else {
      OMS_ERROR("Failed to initialize OBI :{} connection", _socket_path);
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Error selecting tasks: {}", e.what());
  }
}

TEST(ClusterDAO, uds_test)
{
  std::string _socket_path = "../packenv/oblogproxy/run/binlogit/ob_mysql/binlogit$ob_mysql$3/binlog_instance.socket";
  try {
    // Instantiate Driver
    sql::Driver* driver = sql::mariadb::get_driver_instance();

    //    std::string url = "unix://localhost";
    std::string url = "jdbc:mariadb://11.124.9.13:8100";
    sql::Properties properties;
    properties.insert("user", "a");
    properties.insert("password", "b");
    //    properties.insert("localSocket", _socket_path);

    // Establish Connection
    std::unique_ptr<sql::Connection> conn(driver->connect(url, properties));
    if (conn == nullptr) {
      OMS_ERROR("Failed to initialize OBI :{} connection", _socket_path);
    }
    std::shared_ptr<sql::PreparedStatement> statement(conn->prepareStatement("report"));
    if (statement != nullptr) {
      auto* rs = statement->executeQuery();
      while (rs->next()) {
        int64_t delay = rs->getInt64("convert_delay");
        uint64_t minimum_dump_point = rs->getUInt64("min_dump_checkpoint");
        OMS_INFO("delay:{},checkpoint:{}", delay, minimum_dump_point);
      }
    } else {
      OMS_ERROR("Failed to initialize OBI :{} connection", _socket_path);
    }
    std::cout << "I connected successfully!";
  } catch (sql::SQLException& e) {
    OMS_ERROR("Error selecting tasks: {}", e.what());
  }
}