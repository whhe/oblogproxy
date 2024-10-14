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

#include "config.h"
#include "thread.h"
#include "client_meta.h"
#include "obcdc_config.h"

namespace oceanbase::logproxy {

class SourceInvoke {
public:
  static int invoke(Comm&, const ClientMeta&, ObcdcConfig&);

private:
  static int serialize_configs(const ClientMeta& client, const ObcdcConfig& config, const std::string& config_file);

  static int start_oblogreader(Comm& comm, const ClientMeta& client, ObcdcConfig& config);
};

}  // namespace oceanbase::logproxy
