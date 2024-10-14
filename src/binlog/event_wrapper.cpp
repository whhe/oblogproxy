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

#include <sys/un.h>

#include "event_wrapper.h"
#include "log.h"
#include "common.h"
#include "cluster/node.h"

namespace oceanbase::binlog {

struct evconnlistener* evconnlistener_new(struct event_base* ev_base, uint16_t port, evconnlistener_cb cb,
    int& error_no, evconnlistener_errorcb error_cb, int backlog)
{
  struct evconnlistener* listener = nullptr;

  struct sockaddr_in sin {};
  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = INADDR_ANY;
  sin.sin_port = htons(port);
  listener = evconnlistener_new_bind(ev_base,
      cb,
      nullptr,
      LEV_OPT_REUSEABLE | LEV_OPT_CLOSE_ON_FREE | LEV_OPT_CLOSE_ON_EXEC,
      (backlog == 0) ? -1 : backlog,
      reinterpret_cast<const struct sockaddr*>(&sin),
      sizeof(sin));
  if (listener == nullptr) {
    error_no = errno;
    OMS_ERROR("Failed to listen socket with port: {}, error: {}", port, logproxy::system_err(errno));
    return listener;
  }

  OMS_INFO("Succeed to listen socket with port: {}", port);
  evconnlistener_set_error_cb(listener, error_cb);
  return listener;
}

struct evconnlistener* evconnlistener_new_uds(
    struct event_base* ev_base, evconnlistener_cb cb, evconnlistener_errorcb error_cb, int backlog)
{
  struct evconnlistener* listener = nullptr;

  struct sockaddr_un sun {};
  memset(&sun, 0, sizeof(sun));
  sun.sun_family = AF_LOCAL;
  strcpy(sun.sun_path, INSTANCE_SOCKET_PATH);
  unlink(INSTANCE_SOCKET_PATH);

  listener = evconnlistener_new_bind(ev_base,
      cb,
      nullptr,
      LEV_OPT_REUSEABLE | LEV_OPT_CLOSE_ON_FREE | LEV_OPT_CLOSE_ON_EXEC,
      (backlog == 0) ? -1 : backlog,
      reinterpret_cast<const struct sockaddr*>(&sun),
      sizeof(sun));
  if (listener == nullptr) {
    OMS_ERROR(
        "Failed to listen socket on socket path: {}, error: {}", INSTANCE_SOCKET_PATH, logproxy::system_err(errno));
    return listener;
  }

  OMS_INFO("Succeed to listen socket with socket path: {}", INSTANCE_SOCKET_PATH);
  evconnlistener_set_error_cb(listener, error_cb);
  return listener;
}

}  // namespace oceanbase::binlog
