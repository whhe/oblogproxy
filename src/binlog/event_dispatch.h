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

#include "event_wrapper.h"
#include "thread_pool_executor.h"
#include "connection_manager.h"

namespace oceanbase::binlog {
void on_new_connection_cb(
    struct evconnlistener* listener, int sock, struct sockaddr* peer_addr, Connection::ConnectionType conn_type);

void on_new_obm_connection_cb(struct evconnlistener* listener, int sock, struct sockaddr* peer_addr, int, void*);

void on_new_obi_connection_cb(struct evconnlistener* listener, int sock, struct sockaddr* peer_addr, int, void*);

void on_handshake_cb(int sock, short events, void* p_connection);

void on_handshake_response_cb(int sock, short events, void* p_connection);

void on_cmd_cb(int sock, short events, void* p_connection);

}  // namespace oceanbase::binlog
