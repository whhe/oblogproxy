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
#include <unordered_set>
#include "obcdc_config.h"
#include "mysql_protocol.h"

#define FETCH_CLOG_MIN_TS_SQL \
  "SELECT svr_min_log_timestamp FROM oceanbase.__all_virtual_server_clog_stat WHERE zone_status='ACTIVE';"
#define FETCH_CLOG_MIN_TS_SQL_V4 "SELECT CEIL(MAX(BEGIN_SCN)/1000) AS START_TS_US FROM oceanbase.GV$OB_LOG_STAT;"
#define FETCH_ARCHIVE_CLOG_MIN_TS_SQL_V4 \
  "SELECT CEIL(MAX(START_SCN)/1000) as START_TS_US  FROM oceanbase.DBA_OB_ARCHIVELOG;"

namespace oceanbase::logproxy {

class ObAccess {
public:
  struct ServerInfo {
    std::string host;
    int port;
  };

public:
  ObAccess() = default;
  ~ObAccess() = default;

  int init(const ObcdcConfig&, const std::string& password_sha1, const std::string& sys_password_sha1);

  int fetch_connection(MysqlProtocol& mysql_protocol);

  int auth();

  int query_ob_version(const ObcdcConfig&, std::string&);

  int query_server_id(const ObcdcConfig&, std::string& server_id);

  int query_server_uuid(const ObcdcConfig&, const std::string& tenant, std::string& server_uuid);

  int query_cluster_id(const ObcdcConfig&, std::string& cluster_id);

  int query_tenant_id(const ObcdcConfig&, const std::string& tenant,std::string& tenant_id);

  int query_min_clog_timestamp(const ObcdcConfig&, uint64_t& min_clog_timestamp_us);

private:
  int auth_sys(const ServerInfo&);

  int auth_tenant(const ServerInfo&);

  int auth_tables(const std::map<std::string, std::set<std::string>>&, const std::string&, MysqlProtocol&);

private:
  std::vector<ServerInfo> _servers;
  std::string _user;
  std::string _user_to_conn;
  std::string _password_sha1;
  std::string _sys_user;
  std::string _sys_password_sha1;

  // TODO... database table auth
  TenantDbTable _table_whites;
};

struct ObUsername {
  std::string cluster;
  std::string tenant;
  std::string username;

  explicit ObUsername(const std::string& full_name);

  std::string name_without_cluster(const std::string& in_tenant = "");
};

}  // namespace oceanbase::logproxy
