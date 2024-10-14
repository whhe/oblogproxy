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

#include <utility>
#include <netinet/in.h>
#include "transport.h"
#include "log.h"
namespace oceanbase::logproxy {

int GossipTransport::init(TransportConfig config)
{
  _tcp_fd = oceanbase::logproxy::listen(config.address.c_str(), config.port, false, true);
  if (_tcp_fd <= 0) {
    return OMS_FAILED;
  }

  OMS_INFO("Success to init TCP Listener on port: {}, fd: {}", config.port, _tcp_fd);

  _udp_fd = oceanbase::logproxy::listen(config.address.c_str(), config.port, false, true, IPPROTO_UDP);
  if (_udp_fd <= 0) {
    return OMS_FAILED;
  }

  OMS_INFO("Success to init UDP Listener on port: {}, fd: {}", config.port, _udp_fd);

  // Register tcp and udp listeners to listen to data

  return OMS_OK;
}

int Transport::init(TransportConfig config)
{
  return 0;
}

Transport::Transport(TransportConfig config) : _config(std::move(config))
{}
}  // namespace oceanbase::logproxy