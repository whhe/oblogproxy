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

#include "connection.h"

#include <map>
#include <shared_mutex>

namespace oceanbase::binlog {
class ConnectionManager {
public:
  ConnectionManager() = default;
  ~ConnectionManager() = default;

  ConnectionManager(const ConnectionManager&) = delete;
  ConnectionManager(const ConnectionManager&&) = delete;
  ConnectionManager& operator=(const ConnectionManager&) = delete;
  ConnectionManager& operator=(const ConnectionManager&&) = delete;

  void add(Connection* conn)
  {
    std::unique_lock<std::recursive_mutex> exclusive_lock{recursive_mutex_};
    while (connections_.find(next_thread_id_) != connections_.end()) {
      ++next_thread_id_;
      if (next_thread_id_ == 0) {
        ++next_thread_id_;
      }
    }
    conn->set_thread_id(next_thread_id_);
    connections_[next_thread_id_] = conn;
    ++next_thread_id_;
  }

  void remove(Connection* conn)
  {
    std::unique_lock<std::recursive_mutex> exclusive_lock{recursive_mutex_};
    connections_.erase(conn->get_thread_id());
  }

  void get_conn_info(std::vector<ProcessInfo>& conn_info_vec)
  {
    std::unique_lock<std::recursive_mutex> exclusive_lock{recursive_mutex_};
    for (auto conn_pair : connections_) {
      conn_info_vec.emplace_back(conn_pair.second->get_complete_conn_info());
    }
  }

  void kill_connection(uint32_t killed_conn_id, KilledState killed)
  {
    std::unique_lock<std::recursive_mutex> exclusive_lock{recursive_mutex_};
    if (connections_.find(killed_conn_id) == connections_.end()) {
      return;
    }

    Connection* conn = connections_[killed_conn_id];

    OMS_INFO("Kill connection [{}] with command: {}, and killed state: {}",
        conn->trace_id(),
        killed_state_str(killed),
        server_command_names(conn->conn_info().command));
    if (conn->conn_info().command == ServerCommand::sleep) {
      if (killed == KilledState::KILL_CONNECTION) {
        delete conn;
      } else {
        conn->conn_info().command_start_ts = Timer::now_s();
      }
      return;
    }

    conn->kill(killed);
  }

private:
  std::recursive_mutex recursive_mutex_;
  uint32_t next_thread_id_ = 1;
  std::map<uint32_t, Connection*> connections_;
};

}  // namespace oceanbase::binlog
