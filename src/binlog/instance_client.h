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

#include "string"

#include "obaccess/mysql_protocol.h"
#include "obaccess/ob_mysql_packet.h"
#include "conncpp.hpp"
#include "mysql_connecton_wrapper.h"
#include "cluster/node.h"

using namespace oceanbase::logproxy;

namespace oceanbase::binlog {

class InstanceClient {
public:
  InstanceClient() = default;
  ~InstanceClient() = default;

public:
  int init(const BinlogEntry& instance);

  std::string get_server_addr();

  int report(MySQLResultSet& rs);

  int report(MySQLResultSet& rs, uint64_t gtid);

  int query(const std::string& query_sql, sql::ResultSet*& rs);

  int query(const std::string& query_sql, MySQLResultSet& rs);

  int route_query(PacketBuf& payload, MsgBuf& resp_buf);

private:
  int connect();

private:
  MysqlProtocol _instance;

  // true: local OBI, use uds
  // false: remote OBI, use tcp
  bool _is_local;

  // tcp addr
  std::string _host;
  uint32_t _port = -1;

  // uds addr
  std::string _socket_path;

  std::string _username = "OBM";
  std::string _password_sha1;
};

enum ResultSetState { WAITING, ERROR, READY };
struct ResultSet {
  ResultSetState state = ResultSetState::WAITING;
  MySQLResultSet mysql_result_set;
};

class InstanceClientWrapper {
public:
  static void report_asyn(BinlogEntry& instance, ResultSet* result);
};

}  // namespace oceanbase::binlog
