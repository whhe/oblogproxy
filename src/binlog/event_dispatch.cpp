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
#include "env.h"
#include "event_dispatch.h"
#include "socket_util.h"

#include "log.h"

namespace oceanbase::binlog {

//================= functions that will be executed by g_executor =================================>
static void send_handshake_packet(Connection* conn);

static void process_handshake_response(Connection* conn);

static void do_cmd(Connection* conn);
//<================ functions that will be executed by g_executor ==================================

void on_new_connection_cb(
    struct evconnlistener* listener, int sock, struct sockaddr* peer_addr, Connection::ConnectionType conn_type)
{
  std::string local_ip;
  std::string peer_ip;
  uint16_t local_port = 0;
  uint16_t peer_port = 0;
  SocketUtil::get_socket_local_ip_port(sock, local_ip, local_port);
  sa_family_t sa_family = peer_addr->sa_family;
  SocketUtil::get_socket_peer_ip_port(sock, peer_ip, peer_port, peer_addr);
  auto* conn = new Connection{sock,
      std::move(local_ip),
      std::move(peer_ip),
      local_port,
      peer_port,
      sa_family,
      conn_type,
      *g_connection_manager,
      *g_sys_var};
  struct event_base* ev_base = evconnlistener_get_base(listener);
  assert(conn->get_event() == nullptr);
  conn->register_event(EV_WRITE, on_handshake_cb, ev_base);
  OMS_INFO("On connect from {}", conn->endpoint());
}

void on_new_obm_connection_cb(struct evconnlistener* listener, int sock, struct sockaddr* peer_addr, int, void*)
{
  on_new_connection_cb(listener, sock, peer_addr, Connection::ConnectionType::OBM);
}

void on_new_obi_connection_cb(struct evconnlistener* listener, int sock, struct sockaddr* peer_addr, int, void*)
{
  on_new_connection_cb(listener, sock, peer_addr, Connection::ConnectionType::OBI);
}

void on_handshake_cb(int sock, short events, void* p_connection)
{
  const auto conn = reinterpret_cast<Connection*>(p_connection);
  assert(sock == conn->get_sock_fd());
  g_executor->submit(send_handshake_packet, conn);
}

void on_handshake_response_cb(int sock, short events, void* p_connection)
{
  const auto conn = reinterpret_cast<Connection*>(p_connection);
  assert(sock == conn->get_sock_fd());
  g_executor->submit(process_handshake_response, conn);
}

void on_cmd_cb(int sock, short events, void* p_connection)
{
  const auto conn = reinterpret_cast<Connection*>(p_connection);
  assert(sock == conn->get_sock_fd());
  g_executor->submit(do_cmd, conn);
}

//================= functions that will be executed by g_executor =================================>
static void send_handshake_packet(Connection* conn)
{
  if (conn->send_handshake_packet() != Connection::IoResult::SUCCESS) {
    OMS_STREAM_DEBUG << "Failed to send handshake packet on connection " << conn->endpoint() << ", error "
                     << logproxy::system_err(errno);
    delete conn;
    return;
  }
  OMS_STREAM_DEBUG << "Sent handshake on connection " << conn->endpoint();
  conn->register_event(EV_READ, on_handshake_response_cb);
}

static void process_handshake_response(Connection* conn)
{
  if (conn->process_handshake_response() != Connection::IoResult::SUCCESS) {
    OMS_STREAM_DEBUG << "Failed to process handshake response packet on connection " << conn->endpoint() << ", error "
                     << logproxy::system_err(errno);
    delete conn;
    return;
  }
  OMS_INFO("Processed handshake response on connection:{}", conn->endpoint());
  conn->register_event(EV_READ, on_cmd_cb);
}

static void do_cmd(Connection* conn)
{
  std::string endpoint = conn->trace_id();
  Connection::IoResult io_result = conn->do_cmd();
  switch (io_result) {
    case Connection::IoResult::SUCCESS:
      OMS_INFO("Succeeded do_cmd on connection {}", endpoint);
      conn->enter_sleep();
      conn->register_event(EV_READ, on_cmd_cb);
      break;
    case Connection::IoResult::FAIL:
      delete conn;
      break;
    case Connection::IoResult::BINLOG_DUMP:
      // The ownership of conn has been transferred to BinlogDumper, never use conn anymore except in BinlogDumper!!!
      OMS_INFO("Connection {} has been transferred to binlog dump state", endpoint);
      break;
    case Connection::IoResult::QUIT:
      OMS_INFO("Succeeded to quit on connection {}", endpoint);
      delete conn;
      break;
    case Connection::IoResult::KILL:
      OMS_INFO("Succeeded to kill connection {}, type: {}", endpoint, killed_state_str(conn->get_killed_state()));
      if (conn->get_killed_state() == KilledState::KILL_QUERY) {
        conn->enter_sleep();
        conn->register_event(EV_READ, on_cmd_cb);
      } else if (conn->get_killed_state() == KilledState::KILL_CONNECTION) {
        delete conn;
      }
      break;
  }
}
//<================ functions that will be executed by g_executor ==================================

}  // namespace oceanbase::binlog