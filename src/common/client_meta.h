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
#include <ostream>
#include "model.h"
#include "config.h"
#include "communication/comm.h"
#include "codec/message.h"

namespace oceanbase::logproxy {

struct ClientMeta : public Void {
  OMS_MF_ENABLE_COPY(ClientMeta);

public:
  ClientMeta() = default;

  OMS_MF(LogType, type);
  OMS_MF(std::string, id);

  OMS_MF(std::string, ip);
  OMS_MF(std::string, version);
  OMS_MF(std::string, configuration);

  OMS_MF_DFT(int, pid, 0);
  OMS_MF(Peer, peer);

  OMS_MF(time_t, register_time);
  OMS_MF_DFT(bool, enable_monitor, false);
  OMS_MF_DFT(MessageVersion, packet_version, MessageVersion::V2);

public:
  static ClientMeta from_handshake(const Peer&, ClientHandshakeRequestMessage&);

  int init_from_json(const rapidjson::Value&);

  void to_json(rapidjson::PrettyWriter<rapidjson::StringBuffer>&) const;

  const string& to_string() const override;
};

LogStream& operator<<(LogStream& ss, MessageVersion version);

}  // namespace oceanbase::logproxy
