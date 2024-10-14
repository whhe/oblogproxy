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

#include "mysql_connecton_wrapper.h"

namespace oceanbase::logproxy {

int MySQLConnection::connect(const std::string& ip, int port, std::string user, std::string password,
    std::string database, sql::Properties properties)
{
  try {
    std::string url = "jdbc:mariadb://" + ip + ":" + std::to_string(port) + "/" + database;
    properties.insert("user", user);
    properties.insert("password", password);
    properties.insert("minPoolSize", "2");
    properties.insert("maxPoolSize", "32");
    data_source.setUrl(url);
    data_source.setProperties(properties);
    data_source.setUser(user);
    data_source.setPassword(password);
  } catch (sql::SQLException& e) {
    OMS_ERROR("Error Connecting to MariaDB Platform: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

MySQLConnection::~MySQLConnection()
{}

std::unique_ptr<sql::Connection> MySQLConnection::get_conn()
{
  try {
    std::unique_ptr<sql::Connection> connection(data_source.getConnection());
    if (connection != nullptr) {
      return connection;
    } else {
      OMS_ERROR("Failed to connect to mariadb platform");
      return std::unique_ptr<sql::Connection>();
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to connect to mariadb platform: {},url:{}", e.what(), data_source.getUrl());
    return std::unique_ptr<sql::Connection>();
  }
}

int MySQLConnection::connect(
    const std::string& uds, const std::string& user, const std::string& password, sql::Properties properties)
{
  try {
    std::string url = "unix://localhost";
    properties.insert("user", user);
    properties.insert("password", password);
    properties.insert("minPoolSize", "2");
    properties.insert("maxPoolSize", "32");
    properties.insert("localSocket", uds);
    data_source.setUrl(url);
    data_source.setProperties(properties);
    data_source.setUser(user);
    data_source.setPassword(password);
  } catch (sql::SQLException& e) {
    OMS_ERROR("Error Connecting to MariaDB Platform: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int execute_update(MySQLConnection& mysql_conn, const std::string& sql, int32_t& affected_rows)
{
  const auto conn = mysql_conn.get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection.");
    return OMS_FAILED;
  }

  return execute_update(conn, sql, affected_rows);
}

int execute_update(const std::unique_ptr<sql::Connection>& conn, const std::string& update_sql, int32_t& affected_rows)
{
  OMS_DEBUG("Exec sql: {}", update_sql);
  Timer timer;
  try {
    std::shared_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(update_sql));
    execute_update_with_retry(stmt, affected_rows);
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to execute update, sql: {}, error: {}", update_sql, e.what());
    return OMS_FAILED;
  }
  log_slow_sql_if_exists(timer.elapsed(), update_sql);
  return OMS_OK;
}

sql::ResultSet* execute_query_with_retry(const std::shared_ptr<sql::PreparedStatement>& stmt)
{
  sql::ResultSet* ret = nullptr;
  execute_with_retry(execute_query_op, 3, 1, stmt, ret);
  return ret;
}

int execute_update_with_retry(const std::shared_ptr<sql::PreparedStatement>& stmt, int32_t& affected_rows)
{
  return execute_with_retry(execute_update_op, 3, 1, stmt, affected_rows);
}

int execute_query_op(const std::shared_ptr<sql::PreparedStatement>& stmt, sql::ResultSet*& result_set)
{
  try {
    result_set = stmt->executeQuery();
    return OMS_OK;
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to execute query, error: {}", e.what());
    return OMS_FAILED;
  }
}

int execute_update_op(const std::shared_ptr<sql::PreparedStatement>& stmt, int32_t& affected_rows)
{
  try {
    affected_rows = stmt->executeUpdate();
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to execute update, error: {}", e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

}  // namespace oceanbase::logproxy
