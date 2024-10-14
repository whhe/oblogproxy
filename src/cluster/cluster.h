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
#include "config.h"
#include "codec/message.h"
#include "cluster/node.h"
#include "common.h"
#include "cluster_protocol.h"
#include "database_protocol.h"
#include "cluster_config.h"
#include "standalone_protocol.h"

namespace oceanbase::logproxy {

class ClusterProtocolFactory {
  OMS_SINGLETON(ClusterProtocolFactory);
  OMS_AVOID_COPY(ClusterProtocolFactory);
  ~ClusterProtocolFactory();

public:
  /*!
   * @brief Create a cluster node communication according to the configuration
   * @param cluster_config
   * @return
   */
  static ClusterProtocol* create(ClusterConfig* cluster_config, ProtocolMode mode, TaskExecutorCb& task_executor_cb,
      InstanceFailoverCb& recover_cb);
};

}  // namespace oceanbase::logproxy
