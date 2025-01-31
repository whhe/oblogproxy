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

#include "cmd_processor.h"

#include <unordered_map>

#include "env.h"
#include "config.h"
#include "binlog-instance/binlog_dumper.h"
#include "sql_cmd_processor.h"
#include "sql_parser.h"
#include "instance_client.h"

namespace oceanbase::binlog {

static QueryCmdProcessor _s_query_cmd_processor;
static BinlogDumpCmdProcessor _s_binlog_dump_cmd_processor;
static BinlogDumpGtidCmdProcessor _s_binlog_dump_gtid_cmd_processor;
static RegisterSlaveCmdProcessor _s_register_slave_cmd_processor;
static QuitCmdProcessor _s_quit_cmd_processor;

static std::unordered_map<ServerCommand, CmdProcessor*> _obm_supported_cmd_processors = {
    {ServerCommand::query, &_s_query_cmd_processor}};

static std::unordered_map<ServerCommand, CmdProcessor*> _obi_supported_cmd_processors = {
    {ServerCommand::query, &_s_query_cmd_processor},
    {ServerCommand::binlog_dump, &_s_binlog_dump_cmd_processor},
    {ServerCommand::binlog_dump_gtid, &_s_binlog_dump_gtid_cmd_processor},
    {ServerCommand::register_slave, &_s_register_slave_cmd_processor},
    {ServerCommand::quit, &_s_quit_cmd_processor},
};

CmdProcessor* cmd_processor(Connection::ConnectionType conn_type, ServerCommand server_command)
{
  std::unordered_map<ServerCommand, CmdProcessor*> supported_cmd_processors =
      Connection::ConnectionType::OBM == conn_type ? _obm_supported_cmd_processors : _obi_supported_cmd_processors;
  auto iter = supported_cmd_processors.find(server_command);
  if (iter != supported_cmd_processors.end()) {
    return iter->second;
  }
  return nullptr;
}

IoResult QueryCmdProcessor::process(Connection* conn, PacketBuf& payload)
{
  std::string query;
  payload.read_fixed_length_string(query, payload.readable_bytes());
  OMS_INFO("Received query [{}] on connection {}", query, conn->endpoint());

  hsql::SQLParserResult parser_result;
  int ret = ObSqlParser::parse(query, parser_result);
  if (ret != OMS_OK) {
    OMS_WARN("Unsupported sql query [{}], sqlError [{} (L{}:{})] on connection {}",
        query,
        parser_result.errorMsg(),
        parser_result.errorLine(),
        parser_result.errorColumn(),
        conn->endpoint());
    return conn->send_ok_packet();
  }

  if (parser_result.getStatements().empty()) {
    OMS_WARN("sql statement is empty on connection {}", conn->endpoint());
    return conn->send_ok_packet();
  }
  auto* statement = parser_result.getStatement(0);
  OMS_INFO("Successfully parsed SQL [type: {}], [{}]", statement->type(), query);
  SqlCmdProcessor* p_sql_cmd_processor = sql_cmd_processor(conn->get_conn_type(), statement->type());
  if (p_sql_cmd_processor != nullptr) {
    conn->conn_info().info = query;
    return p_sql_cmd_processor->process(conn, statement);
  }

  OMS_WARN("Unsupported sql query: [{}]", query);
  return conn->send_ok_packet();
}

IoResult BinlogDumpCmdProcessor::process(Connection* conn, PacketBuf& payload)
{
  uint32_t binlog_pos = 0;
  uint16_t flags = 0;
  uint32_t slave_server_id = 0;

  payload.read_uint4(binlog_pos);
  payload.read_uint2(flags);
  payload.read_uint4(slave_server_id);
  const uint8_t* binlog_buff = nullptr;
  uint32_t buff_len = payload.read_remaining_bytes(binlog_buff);
  std::string binlog_file((char*)binlog_buff, buff_len);

  OMS_INFO("Start binlog dump on connection {}, file {}, position {}, server_id {}, flags {}",
      conn->endpoint(),
      binlog_file,
      binlog_pos,
      slave_server_id,
      flags);
  if (!g_dumper_manager->can_add_one()) {
    OMS_ERROR("{}: Reached dumper connection limit", conn->trace_id());
    return conn->send_err_packet(BINLOG_FATAL_ERROR, "Insufficient binlog instance resources", "HY000");
  }

  auto* bd = new BinlogDumper(conn);
  bd->set_gtid_mod(false);
  bd->set_file(binlog_file);
  bd->set_start_pos(binlog_pos);
  bd->set_flag(flags);
  bd->set_detach_state(true);
  bd->set_release_state(true);
  uint64_t heartbeat_period = Config::instance().binlog_heartbeat_interval_us.val();
  if (!conn->get_session_var("master_heartbeat_period").empty()) {
    heartbeat_period = atoll(conn->get_session_var("master_heartbeat_period").c_str());
  }
  bd->set_heartbeat_interval_us(heartbeat_period);
  conn->set_server_command(cmd_);
  bd->start();

  return IoResult::BINLOG_DUMP;
}

IoResult BinlogDumpGtidCmdProcessor::process(Connection* conn, PacketBuf& payload)
{
  uint16_t flags = 0;
  uint32_t slave_server_id = 0;
  uint32_t binlog_file_name_size = 0;
  std::string binlog_file_name;
  uint64_t binlog_pos = 0;
  uint32_t gtid_set_excluded_size = 0;
  uint64_t n_sids = 0;

  payload.read_uint2(flags);
  payload.read_uint4(slave_server_id);
  payload.read_uint4(binlog_file_name_size);
  payload.read_fixed_length_string(binlog_file_name, binlog_file_name_size);
  payload.read_uint8(binlog_pos);
  payload.read_uint4(gtid_set_excluded_size);
  gtid_set_excluded_size -= 4;
  payload.read_uint8(n_sids);

  OMS_INFO(
      "Start binlog dump gtid on connection [{}], file [{}], position [{}], server_id [{}], flags [{}], n_sids [{}]",
      conn->endpoint(),
      binlog_file_name,
      binlog_pos,
      slave_server_id,
      flags,
      n_sids);
  if (!g_dumper_manager->can_add_one()) {
    OMS_ERROR("{}: Reached dumper connection limit", conn->trace_id());
    return conn->send_err_packet(BINLOG_FATAL_ERROR, "Insufficient binlog instance resources", "HY000");
  }

  std::map<std::string, GtidMessage*> gtids;
  const uint8_t* gtid_uuid = nullptr;
  for (uint64_t i = 0; i < n_sids; ++i) {
    auto* gtid_message = new GtidMessage();
    payload.read_bytes(gtid_uuid, SERVER_UUID_LEN);
    auto* uuid = static_cast<unsigned char*>(malloc(SERVER_UUID_LEN));
    memset(uuid, 0, SERVER_UUID_LEN);
    memcpy(uuid, gtid_uuid, SERVER_UUID_LEN);
    std::string uuid_str;
    dumphex(reinterpret_cast<const char*>(uuid), SERVER_UUID_LEN, uuid_str);
    OMS_STREAM_INFO << "uuid str: " << uuid_str;
    gtid_message->set_gtid_uuid(uuid);

    uint64_t n_intervals = 0;
    payload.read_uint8(n_intervals);
    OMS_STREAM_INFO << "n_intervals:" << n_intervals;
    gtid_message->set_gtid_txn_id_intervals(n_intervals);

    uint64_t start = 0;
    uint64_t end = 0;
    std::vector<txn_range> txn_ranges;
    for (uint64_t j = 0; j < n_intervals; ++j) {
      payload.read_uint8(start);
      payload.read_uint8(end);
      txn_range range;
      range.first = start;
      range.second = end;
      OMS_STREAM_INFO << "start:" << start << " end:" << end;
      txn_ranges.emplace_back(range);
    }
    gtid_message->set_txn_range(txn_ranges);

    OMS_STREAM_INFO << "gtid message " << gtid_message->format_string();

    gtids.emplace(uuid_str, gtid_message);
  }

  OMS_STREAM_INFO << "Start binlog dump on connection " << conn->endpoint() << ", file " << binlog_file_name
                  << ", position " << binlog_pos << ", server_id " << slave_server_id << ", flags " << flags;

  auto* bd = new BinlogDumper(conn);
  bd->set_gtid_mod(true);
  bd->set_exclude_gtid(gtids);
  bd->set_file(binlog_file_name);
  bd->set_start_pos(binlog_pos);
  bd->set_flag(flags);
  bd->set_detach_state(true);
  bd->set_release_state(true);
  uint64_t heartbeat_period = Config::instance().binlog_heartbeat_interval_us.val();
  if (!conn->get_session_var("master_heartbeat_period").empty()) {
    heartbeat_period = atoll(conn->get_session_var("master_heartbeat_period").c_str());
  }
  bd->set_heartbeat_interval_us(heartbeat_period);
  conn->set_server_command(cmd_);

  bd->start();

  return IoResult::BINLOG_DUMP;
}

IoResult RegisterSlaveCmdProcessor::process(Connection* conn, PacketBuf& payload)
{
  uint32_t server_id;
  uint8_t host_len = 0;
  std::string host;
  uint8_t user_len = 0;
  std::string user;
  uint8_t password_len = 0;
  std::string password;
  uint16_t port;
  uint32_t recovery_rank = 0;
  uint32_t master_id = 0;

  payload.read_uint4(server_id);
  payload.read_uint1(host_len);
  payload.read_fixed_length_string(host, host_len);
  payload.read_uint1(user_len);
  payload.read_fixed_length_string(user, user_len);
  payload.read_uint1(password_len);
  payload.read_fixed_length_string(password, password_len);
  payload.read_uint2(port);
  payload.read_uint4(recovery_rank);
  payload.read_uint4(master_id);

  OMS_STREAM_INFO << "Start register slave on connection " << conn->endpoint() << ", host " << host << ", port " << port
                  << ", server_id " << server_id << ", user " << user;

  // currently do nothing
  return conn->send_ok_packet();
}

IoResult QuitCmdProcessor::process(Connection* conn, PacketBuf& payload)
{
  return conn->send_ok_packet();
}

IoResult InstanceProxyCmdProcessor::process(Connection* conn, PacketBuf& payload)
{
  // !! object「InstanceClient client」 will be moved to conn to maintain a long connection in the future !!
  std::string instance_name = conn->get_user();
  BinlogEntry instance;
  g_cluster->query_instance_by_name(instance_name, instance);

  InstanceClient client;
  if (OMS_OK != client.init(instance)) {
    conn->send_err_packet(BINLOG_FATAL_ERROR, "Failed connect to instance", "HY000");
    return IoResult::FAIL;
  }

  MsgBuf msg_buf;
  int ret = client.route_query(payload, msg_buf);
  if (OMS_OK != ret) {
    conn->send_err_packet(BINLOG_FATAL_ERROR, "Failed route sql to binlog instance", "HY000");
    return IoResult::FAIL;
  }

  for (const MsgBuf::Chunk& chunk : msg_buf.get_chunks()) {
    if (IoResult::SUCCESS != conn->send_mysql_packet(reinterpret_cast<const uint8_t*>(chunk.buffer()), chunk.size())) {
      return IoResult::FAIL;
    }
  }
  return IoResult::SUCCESS;
}

}  // namespace oceanbase::binlog