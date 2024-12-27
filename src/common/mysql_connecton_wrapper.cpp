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

int MySQLConnection::init(const std::string& ip, int port, const std::string& user, const std::string& password,
    const std::string& database, const sql::Properties& properties, int init_pool_size)
{
  std::string url = "jdbc:mariadb://" + ip + ":" + std::to_string(port) + "/" + database;
  _properties = properties;
  _properties.emplace("url", url);
  _properties.emplace("user", user);
  _properties.emplace("password", password);
  _properties.emplace("log", "3");
  _url = url;
  return _pool.init(url, init_pool_size, _properties);
}

MySQLConnection::~MySQLConnection() = default;

sql::Connection* MySQLConnection::get_conn(bool verify)
{
  if (verify) {
    auto conn = _pool.get_connection();
    if (conn != nullptr && !conn->isClosed() && conn->isValid()) {
      return conn;
    } else {
      release_connection(conn);
      OMS_WARN("Failed to obtain connection, try to reconnect");
      return get_conn(true);
    }
  } else {
    return _pool.get_connection();
  }
}
void MySQLConnection::release_connection(sql::Connection* conn)
{
  _pool.release_connection(conn);
}

std::unique_ptr<sql::Connection> MySQLConnection::get_conn(
    const std::string& ip, int port, std::string user, const std::string& password, std::string database)
{
  try {
    sql::Driver* driver = sql::mariadb::get_driver_instance();
    std::string url = "jdbc:mariadb://" + ip + ":" + std::to_string(port) + "/" + database;
    sql::Properties properties;
    properties.emplace("url", url);
    properties.emplace("user", user);
    properties.emplace("password", password);
    std::unique_ptr<sql::Connection> connection(driver->connect(url, properties));
    if (connection != nullptr) {
      return connection;
    } else {
      OMS_ERROR("Failed to connect to mariadb platform");
      return nullptr;
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to connect to mariadb platform: {},url:{}", e.what(), _url);
    return nullptr;
  }
}

int MySQLConnection::init(
    const std::string& uds, const std::string& user, const std::string& password, sql::Properties properties)
{
  _properties = properties;
  std::string url = "unix://localhost";
  _properties.emplace("url", url);
  _properties.emplace("user", user);
  _properties.emplace("password", password);
  _properties.emplace("log", "3");
  _properties.emplace("localSocket", uds);
  _url = url;
  return _pool.init(url, 100, _properties);
}

void MySQLConnectionPool::cleanup_inactive_connections()
{
  std::lock_guard<std::mutex> lock(_pool_mutex);
  auto now = Timer::now();
  OMS_INFO("The number of connections in the connection pool is:{}", _connection_pool.size());
  std::vector<sql::Connection*> to_delete;
  defer(release_vector(to_delete));
  for (auto it = _active_time_map.begin(); it != _active_time_map.end();) {
    if ((now - it->second > _max_inactive_duration_us) || it->first->isClosed() || !it->first->isValid()) {
      to_delete.push_back(it->first);
      it = _active_time_map.erase(it);
    } else {
      ++it;
    }
  }
  for (auto conn : to_delete) {
    _connection_pool.erase(conn);
  }
}

void MySQLConnectionPool::start_cleanup_thread()
{
  _stop_cleanup_thread = false;
  std::thread([=]() {
    while (!_stop_cleanup_thread) {
      this->cleanup_inactive_connections();
      std::this_thread::sleep_for(std::chrono::seconds(10));
    }
    OMS_INFO("Connection pool cleaning thread exits");
  }).detach();
}

MySQLConnectionPool::MySQLConnectionPool() : _driver(nullptr), _stop_cleanup_thread(false)
{}

MySQLConnectionPool::~MySQLConnectionPool()
{
  std::lock_guard<std::mutex> lock(_pool_mutex);  // Lock when destructing to prevent race conditions
  _stop_cleanup_thread = true;
  for (auto conn : _connection_pool) {
    delete conn;
  }
  _connection_pool.clear();
}

int MySQLConnectionPool::init(const std::string& url, int initial_size, sql::Properties& properties)
{
  _url = url;
  _properties = properties;
  try {
    _driver = sql::mariadb::get_driver_instance();
    for (int i = 0; i < initial_size; i++) {
      sql::Connection* conn = _driver->connect(_url, _properties);
      if (conn != nullptr) {
        _connection_pool.insert(conn);
      } else {
        // Handle exception
        OMS_ERROR("Failed to initialize connection pool, try again");
        i--;
      }
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to connect to mariadb platform: {},url:{}", e.what(), _url);
    return OMS_FAILED;
  }
  start_cleanup_thread();  // Start the cleanup thread
  return OMS_OK;
}

sql::Connection* MySQLConnectionPool::get_connection()
{
  std::lock_guard<std::mutex> lock(_pool_mutex);  // Lock while accessing the connection pool
  if (_connection_pool.empty()) {
    try {
      auto conn = _driver->connect(_url, _properties);
      if (conn != nullptr) {
        _active_time_map[conn] = Timer::now();  // Update active time
      }
      return conn;
    } catch (sql::SQLException& e) {
      OMS_ERROR("Failed to connect to mariadb platform: {},url:{}", e.what(), _url);
      return nullptr;
    }
  } else {
    auto conn_it = _connection_pool.begin();
    sql::Connection* conn = *conn_it;
    _connection_pool.erase(conn_it);
    _active_time_map[conn] = Timer::now();
    return conn;
  }
}

void MySQLConnectionPool::release_connection(sql::Connection* conn)
{
  std::lock_guard<std::mutex> lock(_pool_mutex);  // Lock while modifying the pool
  if (conn == nullptr) {
    return;
  }
  try {
    if (conn->isClosed()) {
      _active_time_map.erase(conn);
      delete conn;
      return;
    }
    if (conn->isValid()) {
      conn->reset();  // Reset connection state
      _connection_pool.insert(conn);
      _active_time_map[conn] = Timer::now();  // Update active time
    } else {
      OMS_ERROR("Failed to return connection to pool, conn is invalid");
      conn->close();
      _active_time_map.erase(conn);
      delete conn;
    }
  } catch (sql::SQLException& e) {
    // Handle exception
    OMS_ERROR("Failed to return connection to pool:{}", e.what());
    delete conn;
  }
}
int execute_update(MySQLConnection& mysql_conn, const std::string& sql, int32_t& affected_rows)
{
  const auto conn = mysql_conn.get_conn();
  if (conn == nullptr) {
    OMS_ERROR("Failed to get connection.");
    return OMS_FAILED;
  }
  defer(mysql_conn.release_connection(conn));
  return execute_update(conn, sql, affected_rows);
}

int execute_update(sql::Connection* conn, const std::string& update_sql, int32_t& affected_rows)
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
  execute_with_retry(execute_query_op, 3, 1000, stmt, ret);
  return ret;
}

int execute_update_with_retry(const std::shared_ptr<sql::PreparedStatement>& stmt, int32_t& affected_rows)
{
  return execute_with_retry(execute_update_op, 3, 1000, stmt, affected_rows);
}

int reconnect(const std::shared_ptr<sql::PreparedStatement>& stmt, sql::SQLException& e)
{
  if (stmt->getConnection() != nullptr && (stmt->getConnection()->isClosed() || !stmt->getConnection()->isValid())) {
    OMS_ERROR("Connection is closed or invalid, error: {},reconnect....", e.what());
    if (!stmt->getConnection()->isClosed()) {
      stmt->getConnection()->close();
    }
    stmt->getConnection()->reconnect();
  }
  return OMS_FAILED;
}

int execute_query_op(const std::shared_ptr<sql::PreparedStatement>& stmt, sql::ResultSet*& result_set)
{
  try {
    result_set = stmt->executeQuery();
    return OMS_OK;
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to execute query, error: {} , error code:{}", e.getMessage(), e.getErrorCode());
    // return reconnect(stmt, e);
    return OMS_FAILED;
  }
}

int execute_update_op(const std::shared_ptr<sql::PreparedStatement>& stmt, int32_t& affected_rows)
{
  try {
    affected_rows = stmt->executeUpdate();
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to execute update, error: {}", e.what());
    // return reconnect(stmt, e);
    return OMS_FAILED;
  }
  return OMS_OK;
}

}  // namespace oceanbase::logproxy
