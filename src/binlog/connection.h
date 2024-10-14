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

#include <cassert>
#include <cstdint>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include "event_wrapper.h"
#include "mysql-protocol/mysql_protocol_new.h"
#include "sys_var.h"
#include "cluster/node.h"
#include "obaccess/ob_access.h"
#include "common/timer.h"
#include "password.h"

#include <unistd.h>

namespace oceanbase::binlog {
class ConnectionManager;

#define BINLOG_FATAL_ERROR 1236
#define BINLOG_ERROR_WHEN_EXECUTING_COMMAND 1220
#define SYNTAX_ERROR 1064
#define BINLOG_ACCESS_DENIED_ERROR 1045

enum ProcessState { SEND_EVENT, WAIT_EVENT, INITIAL, P_UNDEF };
std::string process_state_info(ProcessState state);

struct ProcessInfo {
  uint32_t id;
  std::string user;
  std::string host;
  std::string real_host;
  std::string db;
  ServerCommand command = ServerCommand::init;
  uint32_t command_start_ts;
  ProcessState state = ProcessState::P_UNDEF;
  std::string info;

  void update_proc_state(ServerCommand server_command)
  {
    command = server_command;
    command_start_ts = Timer::now_s();
  }

  void reset(ServerCommand server_command)
  {
    command = server_command;
    command_start_ts = Timer::now_s();
    state = ProcessState::P_UNDEF;
    info = "";
  }
};

enum KilledState { NOT_KILLED, KILL_BAD_DATA, KILL_CONNECTION, KILL_QUERY, KILL_TIMEOUT, KILLED_NO_VALUE };
std::string killed_state_str(KilledState killed);

class Connection {
public:
  struct AuthInfo {
    std::string user;
    std::string password;
    char scramble[SCRAMBLE_LENGTH + 1];
    bool passwd_used = true;

    std::string password_used()
    {
      return passwd_used ? "YES" : "NO";
    }
  };

  enum class IoResult {
    SUCCESS,
    FAIL,
    BINLOG_DUMP,
    QUIT,
    KILL,
  };

  enum class ConnectionType { OBM, OBI };
  static std::string to_string(ConnectionType type)
  {
    switch (type) {
      case ConnectionType::OBM:
        return "OBM";
      case ConnectionType::OBI:
        return "OBI";
      default:
        return "Unknown";
    }
  }

public:
  Connection(int sock_fd, std::string local_ip, std::string peer_ip, uint16_t local_port, uint16_t peer_port,
      sa_family_t sa_family, ConnectionType conn_type, ConnectionManager& conn_mgr, const SysVar& sys_var);

  ~Connection();

  Connection(const Connection&) = delete;
  Connection(const Connection&&) = delete;
  Connection& operator=(const Connection&) = delete;
  Connection& operator=(const Connection&&) = delete;

  int get_sock_fd() const
  {
    return sock_fd_;
  }

  uint32_t get_thread_id() const
  {
    return thread_id_;
  }

  const std::string& get_local_ip() const
  {
    return local_ip_;
  }

  const std::string& get_peer_ip() const
  {
    return peer_ip_;
  }

  uint16_t get_local_port() const
  {
    return local_port_;
  }

  uint16_t get_peer_port() const
  {
    return peer_port_;
  }

  struct event* get_event() const
  {
    return ev_;
  }

  const std::string& get_user() const
  {
    return user_;
  }

  const std::string& get_ob_cluster() const
  {
    return ob_cluster_;
  }

  const std::string& get_ob_tenant() const
  {
    return ob_tenant_;
  }

  ConnectionType get_conn_type() const
  {
    return _conn_type;
  }

  std::string get_session_var(const std::string& var_name);

  std::map<std::string, std::string>& get_session_var();

  void set_session_var(const std::string& var_name, std::string var_value);

  const std::string& get_ob_user() const
  {
    return ob_user_;
  }

  void set_ob_cluster(const std::string& ob_cluster);

  void set_ob_tenant(const std::string& ob_tenant);

  ServerCommand get_server_command()
  {
    return _server_command;
  }

  ProcessInfo& conn_info()
  {
    return _conn_info;
  }

  ProcessInfo get_complete_conn_info()
  {
    _conn_info.id = thread_id_;
    _conn_info.user = user_;
    _conn_info.host = peer_ip_ + ":" + std::to_string(peer_port_);
    if (!_real_port.empty() && !_real_port.empty()) {
      _conn_info.real_host = _real_host + ":" + _real_port;
    }
    return _conn_info;
  }

  bool killed()
  {
    return (_killed == KilledState::KILL_QUERY || _killed == KilledState::KILL_CONNECTION) ? true : false;
  }

  KilledState get_killed_state()
  {
    return _killed;
  }

  void enter_sleep()
  {
    _conn_info.reset(ServerCommand::sleep);
    _killed = KilledState::NOT_KILLED;
  }

  void set_server_command(ServerCommand server_command);

  std::string endpoint() const
  {
    return "[" + to_string(get_conn_type()) + "][" + get_user() + "," + get_ob_cluster() + "," + get_ob_tenant() + "]" +
           local_ip_ + ":" + std::to_string(local_port_) + "-" + peer_ip_ + ":" + std::to_string(peer_port_) + "/" +
           std::to_string(sock_fd_);
  }

  std::string trace_id() const
  {
    return "[" + _trace_id + "]" + endpoint();
  }

  void register_event(short events, event_callback_fn cb, struct event_base* ev_base = nullptr);

  void set_client_capabilities(Capability client_capabilities)
  {
    client_capabilities_ = client_capabilities;
  }

  void set_thread_id(uint32_t thread_id)
  {
    thread_id_ = thread_id;
  }

  bool is_admin() const
  {
    return _is_admin;
  }

  void kill(KilledState killed);

  IoResult send(const DataPacket& data_packet);

  IoResult send_handshake_packet();

  IoResult send_result_metadata(const std::vector<ColumnPacket>& column_packets);

  IoResult process_handshake_response();

  IoResult do_cmd();

  IoResult send_ok_packet();

  IoResult send_ok_packet(uint16_t warnings, const std::string& info = "");

  IoResult send_ok_packet(uint64_t affected_rows, uint64_t last_insert_id, ServerStatus status_flags, uint16_t warnings,
      const std::string& info = "");

  IoResult send_eof_packet();

  IoResult send_eof_packet(uint16_t warnings, ServerStatus status_flags);

  IoResult send_err_packet(uint16_t err_code, std::string err_msg, const std::string& sql_state);

  IoResult send_binlog_event(const uint8_t* event_buf, uint32_t len);

  std::string get_full_binlog_path() const;

  void start_row()
  {
    pkt_buf_.clear();
  }

  ssize_t store_string(const std::string& value, bool prefix_with_encoded_len = true)
  {
    return pkt_buf_.write_string(value, prefix_with_encoded_len);
  }

  ssize_t store_uint64(uint64_t value)
  {
    return store_string(std::to_string(value));
  }

  ssize_t store_null()
  {
    return pkt_buf_.write_uint1(0xFB);
  };

  IoResult send_row()
  {
    if (killed()) {
      if (_killed == KilledState::KILL_QUERY) {
        send_err_packet(BINLOG_ERROR_WHEN_EXECUTING_COMMAND, "Query execution was interrupted", "HY000");
      }
      return IoResult::KILL;
    }

    return send_data_packet();
  }

  IoResult send_mysql_packet(const uint8_t* payload, uint32_t payload_length);

private:
  IoResult read_data_packet();

  IoResult send_data_packet();

  IoResult read_mysql_packet(uint32_t& payload_length);

  std::string connect_attrs_str() const
  {
    std::string attr_str;
    if (_connect_attrs.empty()) {
      return "";
    }
    for (const auto& attr_kv : _connect_attrs) {
      attr_str.append(attr_kv.first).append(":").append(attr_kv.second).append(",");
    }
    return attr_str.erase(attr_str.size() - 1);
  }

  IoResult auth();

private:
  static constexpr uint8_t mysql_pkt_header_length = 4;
  static constexpr uint32_t mysql_pkt_max_length = (1U << 24) - 1;
  static const std::regex ob_full_user_name_pattern;

private:
  const int sock_fd_;
  const std::string local_ip_;
  const std::string peer_ip_;
  const uint16_t local_port_;
  const uint16_t peer_port_;
  const sa_family_t sa_family_ = AF_UNSPEC;
  ConnectionManager& conn_mgr_;
  uint32_t thread_id_;
  ConnectionType _conn_type;

  // set on handshake response phase
  Capability client_capabilities_;
  bool _is_admin = true;  // temp
  std::string user_;      // full username
  std::string ob_cluster_;
  std::string ob_tenant_;
  std::string ob_user_;

  std::map<std::string, std::string> _connect_attrs;
  std::string _real_host;
  std::string _real_port;

  struct event* ev_;

  PacketBuf pkt_buf_;
  uint8_t seq_no_;

  std::string _trace_id;

  ServerCommand _server_command = ServerCommand::init;

  /* session variables */
  uint32_t net_buffer_length_;
  uint32_t max_allowed_packet_;
  uint32_t net_read_timeout_;
  uint32_t net_write_timeout_;
  uint64_t net_retry_count_;
  std::map<std::string, std::string> _session_vars;

  ProcessInfo _conn_info;
  KilledState _killed = KilledState::NOT_KILLED;

  AuthInfo _auth_info;
};

}  // namespace oceanbase::binlog
