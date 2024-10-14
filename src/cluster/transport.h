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
#include "communication/io.h"
#include "common.h"
#include "log.h"

namespace oceanbase::logproxy {

struct TransportConfig {
  std::string address;
  uint16_t port;
};

class Transport {
public:
  explicit Transport(TransportConfig config);
  Transport() = default;

private:
  TransportConfig _config;

public:
  virtual int init(TransportConfig config);
};

class GossipTransport : public Transport {
private:
  int _tcp_fd = 0;
  int _udp_fd = 0;

public:
  /*
   * @params config,transport config
   * @returns
   * @description
   * @date 2023/1/9 17:13
   */
  int init(TransportConfig config) override;
  GossipTransport() = default;
};

}  // namespace oceanbase::logproxy
