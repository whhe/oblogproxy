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

#include "instance_client.h"

#include "log.h"
#include "msg_buf.h"
#include "env.h"

namespace oceanbase::binlog {

int InstanceClient::init(const BinlogEntry& instance)
{
  if (strcmp(instance.node_id().c_str(), g_cluster->get_node_info().id().c_str()) == 0) {
    std::string socket_path;
    socket_path.append(instance.work_path()).append("/").append(INSTANCE_SOCKET_PATH);
    _socket_path = socket_path;
    _is_local = true;
  } else {
    _host = instance.ip();
    _port = instance.port();
    _username = instance.config()->binlog_config()->instance_user();
    std::string password_sha1;
    hex2bin(instance.config()->binlog_config()->instance_password_sha1().c_str(),
        instance.config()->binlog_config()->instance_password_sha1().size(),
        password_sha1);
    _password_sha1 = password_sha1;
    _is_local = false;
  }

  _instance.set_detect_timeout(10000);
  if (_instance.get_server_addr().empty() && OMS_OK != connect()) {
    OMS_ERROR(
        "Failed to connect to instance: {}, server addr: {}", instance.instance_name(), _instance.get_server_addr());
    return OMS_FAILED;
  }
  return OMS_OK;
}

int InstanceClient::connect()
{
  bool exceed_uds_len_limit = _socket_path.length() > 128;
  int ret = OMS_OK;
  std::string server_addr;
  if (_is_local && !exceed_uds_len_limit) {
    ret = _instance.login(_socket_path, _username);
    server_addr = _socket_path;
  } else {
    ret = _instance.login(_host, _port, _username, _password_sha1, "");
    server_addr = std::string(_host) + std::string(":") + std::to_string(_port);
  }
  if (OMS_OK != ret) {
    OMS_ERROR("Failed to login binlog instance, is_local: {}, addr: {}, exceed uds len limit: {}",
        _is_local,
        server_addr,
        exceed_uds_len_limit);
    return OMS_FAILED;
  }

  OMS_DEBUG("Successfully login binlog instance, is_local: {}, addr: {}", _is_local, server_addr);
  return OMS_OK;
}

int InstanceClient::report(MySQLResultSet& rs)
{
  return _instance.query("report", rs);
}

int InstanceClient::report(MySQLResultSet& rs, uint64_t timestamp)
{
  return _instance.query("report " + std::to_string(timestamp), rs);
}

int InstanceClient::query(const std::string& query_sql, MySQLResultSet& rs)
{
  return _instance.query(query_sql, rs);
}

int InstanceClient::route_query(PacketBuf& payload, MsgBuf& resp_buf)
{
  MsgBuf request_buf;
  request_buf.push_back_copy((char*)payload.get_buf(), payload.readable_bytes());
  return _instance.route_query(request_buf, resp_buf);
}

std::string InstanceClient::get_server_addr()
{
  return _instance.get_server_addr();
}

int InstanceClient::query(const string& query_sql, sql::ResultSet*& rs)
{
  MySQLConnection connection;
  try {
    auto conn = connection.get_conn(_host, _port, _username, "", "");
    if (conn == nullptr) {
      OMS_ERROR("Failed to initialize OBI :{} connection", _socket_path);
      return OMS_FAILED;
    }
    defer(conn->close());
    std::shared_ptr<sql::PreparedStatement> statement(conn->prepareStatement(query_sql));
    if (statement != nullptr) {
      rs = statement->executeQuery();
    } else {
      OMS_ERROR("Failed to construct prepare Statement :{} connection", _socket_path);
      return OMS_FAILED;
    }
  } catch (sql::SQLException& e) {
    OMS_ERROR("Failed to execute sql [{}] to OBI,msg:{}", query_sql, e.what());
    return OMS_FAILED;
  }
  return OMS_OK;
}

void InstanceClientWrapper::report_asyn(BinlogEntry& instance, ResultSet* result)
{
  if (instance.can_connected()) {
    InstanceClient client;
    if (OMS_OK != client.init(instance)) {
      OMS_ERROR("Unable to connect to binlog instance [{}]", instance.instance_name());
    } else {
      if (OMS_OK != client.report(result->mysql_result_set)) {
        OMS_ERROR("Failed to obtain the convert metrics of binlog instance: {}", instance.instance_name());
      } else {
        result->state = ResultSetState::READY;
      }
    }
  }

  if (result->state == ResultSetState::WAITING) {
    result->state = ResultSetState::ERROR;
  }
}

}  // namespace oceanbase::binlog
