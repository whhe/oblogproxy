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

#include "connection.h"

#include <utility>
#include "connection_manager.h"

#include "cmd_processor.h"
#include "config.h"

#include "log.h"
#include "communication/io.h"
#include "common_util.h"

#include <env.h>
#include <metric/prometheus.h>

namespace oceanbase::binlog {

using IoResult = Connection::IoResult;
std::string process_state_info(ProcessState state)
{
  std::vector<std::string> _state_info = {
      "Send event", "Master has sent all binlog to slave; waiting for binlog to be updated", "Init"};
  if (static_cast<uint8_t>(state) >= static_cast<uint8_t>(ProcessState::SEND_EVENT) &&
      static_cast<uint8_t>(state) <= static_cast<uint8_t>(ProcessState::INITIAL)) {
    return _state_info[static_cast<size_t>(state)];
  }
  return "";
}

std::string killed_state_str(KilledState killed)
{
  std::vector<std::string> _state_info = {
      "Not killed", "Kill bad data", "Kill connection", "Kill query", "Kill timeout", "Kill no value"};
  if (static_cast<uint8_t>(killed) >= static_cast<uint8_t>(KilledState::NOT_KILLED) &&
      static_cast<uint8_t>(killed) <= static_cast<uint8_t>(KilledState::KILLED_NO_VALUE)) {
    return _state_info[static_cast<size_t>(killed)];
  }
  return "Unknown";
}

const std::regex Connection::ob_full_user_name_pattern{R"((.*)@(.*)#(.*))"};  // user@tenant#cluster

Connection::Connection(int sock_fd, std::string local_ip, std::string peer_ip, uint16_t local_port, uint16_t peer_port,
    sa_family_t sa_family, ConnectionType conn_type, ConnectionManager& conn_mgr, const SysVar& sys_var)
    : sock_fd_(sock_fd),
      local_ip_(std::move(local_ip)),
      peer_ip_(std::move(peer_ip)),
      local_port_(local_port),
      peer_port_(peer_port),
      sa_family_(sa_family),
      conn_mgr_(conn_mgr),
      client_capabilities_(ob_binlog_server_capabilities) /* for handshake */,
      thread_id_(0),
      ev_(nullptr),
      pkt_buf_(sys_var.net_buffer_length, sys_var.max_allowed_packet),
      seq_no_(0),
      net_buffer_length_(sys_var.net_buffer_length),
      max_allowed_packet_(sys_var.max_allowed_packet),
      net_read_timeout_(sys_var.net_read_timeout),
      net_write_timeout_(sys_var.net_write_timeout),
      net_retry_count_(sys_var.net_retry_count),
      _trace_id(CommonUtils::generate_trace_id()),
      _conn_type(conn_type)
{
  _conn_info.update_proc_state(ServerCommand::init);
  conn_mgr_.add(this);
}

Connection::~Connection()
{
  conn_mgr_.remove(this);
  if (ev_ != nullptr) {
    event_free(ev_);
    ev_ = nullptr;
  }
  close(sock_fd_);
  OMS_INFO("Closed connection {}", endpoint());
}

void Connection::register_event(short events, event_callback_fn cb, struct event_base* ev_base)
{
  if (ev_ == nullptr) {
    assert(ev_base != nullptr);
    ev_ = event_new(ev_base, sock_fd_, events, cb, this);
    event_add(ev_, nullptr);
  } else {
    event_callback_fn old_cb = event_get_callback(ev_);
    short old_events = event_get_events(ev_);
    if (old_cb == cb && old_events == events) {
      if (!(old_events & EV_PERSIST)) {
        event_add(ev_, nullptr);
      }
    } else {
      ev_base = (ev_base == nullptr) ? event_get_base(ev_) : ev_base;
      event_free(ev_);
      ev_ = event_new(ev_base, sock_fd_, events, cb, this);
      event_add(ev_, nullptr);
    }
  }
}

IoResult Connection::send(const DataPacket& data_packet)
{
  pkt_buf_.clear();
  data_packet.serialize(pkt_buf_, client_capabilities_);
  return send_data_packet();
}

IoResult Connection::read_data_packet()
{
  pkt_buf_.clear();
  uint32_t payload_length;
  do {
    if (read_mysql_packet(payload_length) != IoResult::SUCCESS) {
      return IoResult::FAIL;
    }
  } while (payload_length == mysql_pkt_max_length);
  return IoResult::SUCCESS;
}

IoResult Connection::read_mysql_packet(uint32_t& payload_length)
{
  uint8_t header[mysql_pkt_header_length];
  int err;
  if ((err = logproxy::readn(sock_fd_, header, mysql_pkt_header_length)) < 0) {
    if (err == OMS_CLIENT_CLOSED) {
      OMS_INFO("Client [{}] has been closed.", endpoint());
    } else {
      OMS_ERROR("Can not read response from client. Expected to read 4 bytes, read 0 bytes before connection was "
                "unexpectedly lost.");
    }
    return IoResult::FAIL;
  }
  uint8_t sequence = 0;
  uint32_t read_index = 0;
  read_le24toh(header, read_index, payload_length);
  read_le8toh(header, read_index, sequence);
  if (sequence != seq_no_) {
    // TODO: set malformed packet connection error
    OMS_ERROR("Unexpected seq num, expected value is {}, actual value is {}", seq_no_, sequence);
    return IoResult::FAIL;
  }

  if (!pkt_buf_.ensure_writable(payload_length)) {
    // TODO: set max allowed packet error
    return IoResult::FAIL;
  }

  if (logproxy::readn(sock_fd_, pkt_buf_.get_buf() + pkt_buf_.get_write_index(), static_cast<int>(payload_length)) <
      0) {
    return IoResult::FAIL;
  }
  pkt_buf_.set_write_index(pkt_buf_.get_write_index() + payload_length);

  ++seq_no_;
  return IoResult::SUCCESS;
}

IoResult Connection::send_data_packet()
{
  const uint8_t* payload = nullptr;
  while (pkt_buf_.readable_bytes() >= mysql_pkt_max_length) {
    pkt_buf_.read_bytes(payload, mysql_pkt_max_length);
    if (send_mysql_packet(payload, mysql_pkt_max_length) != IoResult::SUCCESS) {
      return IoResult::FAIL;
    }
  }
  uint32_t payload_length = pkt_buf_.read_remaining_bytes(payload);
  return send_mysql_packet(payload, payload_length);
}

IoResult Connection::send_mysql_packet(const uint8_t* payload, uint32_t payload_length)
{
  assert(payload_length <= mysql_pkt_max_length);
  uint8_t header[mysql_pkt_header_length];
  uint32_t write_index = 0;
  write_htole24(header, write_index, payload_length);
  write_htole8(header, write_index, seq_no_++);
  assert(write_index == mysql_pkt_header_length);
  if (logproxy::writen(sock_fd_, header, mysql_pkt_header_length) < 0 ||
      logproxy::writen(sock_fd_, payload, static_cast<int>(payload_length)) < 0) {
    return IoResult::FAIL;
  }
  return IoResult::SUCCESS;
}

IoResult Connection::send_handshake_packet()
{
  uint8_t character_set_nr = 33;  // TODO: utf8_general_ci
  HandshakePacket handshake_pkt{thread_id_, character_set_nr};
  handshake_pkt.get_scramble(_auth_info.scramble);
  return send(handshake_pkt);
}

// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html
IoResult Connection::process_handshake_response()
{
  if (read_data_packet() != IoResult::SUCCESS) {
    return IoResult::FAIL;
  }

  uint32_t client_flags = 0;
  pkt_buf_.read_uint4(client_flags);
  auto client_capabilities = static_cast<Capability>(client_flags);
  if (!has_capability(client_capabilities, Capability::client_protocol_41)) {
    // TODO: only support Protocol::HandshakeResponse41
    return IoResult::FAIL;
  }
  // set the connection client capabilities
  client_capabilities_ = client_capabilities_ & client_capabilities;

  uint32_t max_packet_size;
  pkt_buf_.read_uint4(max_packet_size);

  uint8_t character_set_nr;
  pkt_buf_.read_uint1(character_set_nr);

  constexpr uint8_t zero_filter_length = 23;
  pkt_buf_.read_skip(zero_filter_length);

  // set the connection user
  pkt_buf_.read_null_terminated_string(user_);
  OMS_INFO("Received handshake response on connection {}, user: {}", endpoint(), get_user());
  std::smatch sm;
  if (std::regex_match(user_, sm, ob_full_user_name_pattern)) {
    assert(sm.size() == 4);
    ob_user_ = sm[1].str();
    ob_tenant_ = sm[2].str();
    ob_cluster_ = sm[3].str();
    _is_admin = false;
  } else {
    _is_admin = true;
    OMS_INFO("The user {} on connection {} is not a valid ob username", user_, endpoint());
  }

  std::string auth_response;
  if (has_capability(client_capabilities, Capability::client_plugin_auth_lenenc_client_data)) {
    pkt_buf_.read_length_encoded_string(auth_response);
  } else {
    uint8_t auth_response_length = 0;
    pkt_buf_.read_uint1(auth_response_length);
    pkt_buf_.read_fixed_length_string(auth_response, auth_response_length);
  }

  std::string database;
  if (has_capability(client_capabilities, Capability::client_connect_with_db)) {
    pkt_buf_.read_null_terminated_string(database);
  }

  std::string client_plugin_name;
  if (has_capability(client_capabilities, Capability::client_plugin_auth)) {
    pkt_buf_.read_null_terminated_string(client_plugin_name);
  }

  if (has_capability(client_capabilities, Capability::client_connect_attrs)) {
    uint64_t attrs_len = 0;
    pkt_buf_.read_uint(attrs_len);

    while (attrs_len > 0) {
      std::string key;
      attrs_len -= pkt_buf_.read_length_encoded_string(key);
      std::string value;
      attrs_len -= pkt_buf_.read_length_encoded_string(value);
      _connect_attrs.emplace(key, value);
      if (strcasecmp(key.c_str(), "__client_ip") == 0) {
        _real_host = value;
      }
      if (strcasecmp(key.c_str(), "__client_addr_port") == 0) {
        _real_port = value;
      }
    }
    OMS_INFO("Received {} connect attrs on connection {}: {}", _connect_attrs.size(), endpoint(), connect_attrs_str());
  }

  if (s_config.enable_auth.val() && (_conn_type != ConnectionType::OBI || sa_family_ != AF_LOCAL)) {
    _auth_info.user = user_;
    _auth_info.password = auth_response;
    _auth_info.passwd_used = auth_response.empty() ? false : true;

    OMS_INFO("Auth user {} with password({}), client_plugin_name: {}",
        user_,
        _auth_info.passwd_used ? "true" : "false",
        client_plugin_name);
    if (strcmp(client_plugin_name.c_str(), "mysql_native_password") != 0) {
      return send_err_packet(BINLOG_ACCESS_DENIED_ERROR, "Unsupported client plugin: " + client_plugin_name, "28000");
    }
    return auth();
  }
  OMS_INFO("Skipped auth for user [{}], enable_ath: {}, sa_family: {} ", user_, s_config.enable_auth.val(), sa_family_);
  return send_ok_packet();
}

IoResult Connection::do_cmd()
{
  seq_no_ = 0;  // reset seq_no_ on each command
  if (read_data_packet() != IoResult::SUCCESS) {
    OMS_DEBUG("Failed to do_cmd on connection {}, error {}", endpoint(), logproxy::system_err(errno));
    return IoResult::FAIL;
  }
  if (get_conn_type() == Connection::ConnectionType::OBM && is_admin()) {
    PrometheusExposer::mark_binlog_counter_metric(
        "", "", get_ob_cluster(), get_ob_tenant(), {}, {}, BINLOG_SQL_REQUEST_COUNT_TYPE);
  }
  // route packet to OBI
  if (Config::instance().enable_instance_proxy.val()) {
    if (get_conn_type() == Connection::ConnectionType::OBM && !is_admin()) {
      OMS_INFO("OBM receive query belong to binlog instance, user: {}", get_user());
      return InstanceProxyCmdProcessor::process(this, pkt_buf_);
    }
  }

  uint8_t command = 0;
  pkt_buf_.read_uint1(command);

  auto server_command = static_cast<ServerCommand>(command);
  auto p_cmd_processor = cmd_processor(_conn_type, server_command);
  if (p_cmd_processor != nullptr) {
    _conn_info.update_proc_state(server_command);
    _conn_info.state = ProcessState::INITIAL;
    return p_cmd_processor->process(this, pkt_buf_);
  }
  return UnsupportedCmdProcessor(server_command).process(this, pkt_buf_);
}

IoResult Connection::send_ok_packet()
{
  return send_ok_packet(0, 0, ob_binlog_server_status_flags, 0, "");
}

IoResult Connection::send_ok_packet(uint16_t warnings, const std::string& info)
{
  return send_ok_packet(0, 0, ob_binlog_server_status_flags, warnings, info);
}

IoResult Connection::send_ok_packet(uint64_t affected_rows, uint64_t last_insert_id, ServerStatus status_flags,
    uint16_t warnings, const std::string& info)
{
  OkPacket ok_packet{affected_rows, last_insert_id, status_flags, warnings, info};
  return send(ok_packet);
}

IoResult Connection::send_eof_packet()
{
  return send_eof_packet(0, ob_binlog_server_status_flags);
}

IoResult Connection::send_eof_packet(uint16_t warnings, ServerStatus status_flags)
{
  EofPacket eof_packet{warnings, status_flags};
  return send(eof_packet);
}

IoResult Connection::send_err_packet(uint16_t err_code, std::string err_msg, const std::string& sql_state)
{
  ErrPacket err_packet{err_code, std::move(err_msg), sql_state};
  return send(err_packet);
}

IoResult Connection::send_binlog_event(const uint8_t* event_buf, uint32_t len)
{
  while (len >= mysql_pkt_max_length) {
    if (send_mysql_packet(event_buf, mysql_pkt_max_length) != IoResult::SUCCESS) {
      return IoResult::FAIL;
    }
    event_buf += mysql_pkt_max_length;
    len -= mysql_pkt_max_length;
  }
  return send_mysql_packet(event_buf, len);
}

std::string Connection::get_full_binlog_path() const
{
  return logproxy::Config::instance().binlog_log_bin_basename.val() + "/" + get_ob_cluster() + "/" + get_ob_tenant();
}

IoResult Connection::send_result_metadata(const std::vector<ColumnPacket>& column_packets)
{
  ColumnCountPacket column_count_packet{static_cast<uint16_t>(column_packets.size())};
  if (send(column_count_packet) != IoResult::SUCCESS) {
    return IoResult::FAIL;
  }

  for (const auto& column_packet : column_packets) {
    if (send(column_packet) != IoResult::SUCCESS) {
      return IoResult::FAIL;
    }
  }

  return send_eof_packet();
}

void Connection::set_ob_cluster(const std::string& ob_cluster)
{
  ob_cluster_ = ob_cluster;
}

void Connection::set_ob_tenant(const std::string& ob_tenant)
{
  ob_tenant_ = ob_tenant;
}

void Connection::set_server_command(ServerCommand server_command)
{
  _server_command = server_command;
}

std::string Connection::get_session_var(const std::string& var_name)
{
  if (_session_vars.find(var_name) != _session_vars.end()) {
    return _session_vars[var_name];
  }
  return "";
}

void Connection::set_session_var(const std::string& var_name, std::string var_value)
{
  _session_vars[var_name] = std::move(var_value);
}

std::map<std::string, std::string>& Connection::get_session_var()
{
  return _session_vars;
}

void Connection::kill(KilledState killed)
{
  _killed = killed;
  _conn_info.update_proc_state(ServerCommand::process_kill);
}

IoResult Connection::auth()
{
  std::string password_hex;
  if (_conn_type == ConnectionType::OBM) {
    User user;
    if (OMS_OK != g_cluster->query_user_by_name(_auth_info.user, user)) {
      return send_err_packet(BINLOG_ACCESS_DENIED_ERROR, "Internal error, please try again", "28000");
    }
    if (user.username().empty()) {
      OMS_ERROR("Unknown user: {}", _auth_info.user);
      return send_err_packet(BINLOG_ACCESS_DENIED_ERROR, "Invalid user", "28000");
    }
    password_hex = user.password();
  } else {
    if (strcmp(s_meta.binlog_config()->instance_user().c_str(), _auth_info.user.c_str()) != 0) {
      OMS_ERROR("Unknown user: {}", _auth_info.user);
      return send_err_packet(BINLOG_ACCESS_DENIED_ERROR, "Invalid user", "28000");
    }
    password_hex = s_meta.binlog_config()->instance_password();
  }
  std::string password_sha2;
  hex2bin(password_hex.c_str(), password_hex.size(), password_sha2);

  if (!_auth_info.passwd_used && password_sha2.empty()) {
    OMS_INFO("Auth passed for user [{}] with empty password", _auth_info.user);
    return send_ok_packet();
  } else if ((_auth_info.passwd_used && password_sha2.empty()) || (!_auth_info.passwd_used && !password_sha2.empty())) {
    OMS_ERROR("The auth password or user password for user [{}] is empty, using password: {}",
        _auth_info.user,
        _auth_info.password_used());
    return send_err_packet(BINLOG_ACCESS_DENIED_ERROR,
        "Access denied for user " + _auth_info.user + " (using password: " + _auth_info.password_used() + ")",
        "28000");
  } else if (!check_scramble_sha1(_auth_info.password.c_str(), _auth_info.scramble, password_sha2.c_str())) {
    OMS_ERROR("Auth failed for user [{}] with password", _auth_info.user);
    return send_err_packet(BINLOG_ACCESS_DENIED_ERROR,
        "Access denied for user " + _auth_info.user + " (using password: " + _auth_info.password_used() + ")",
        "28000");
  }
  OMS_INFO("Auth passed for user {}, using password: {}", _auth_info.user, _auth_info.password_used());
  return send_ok_packet();
}

}  // namespace oceanbase::binlog
