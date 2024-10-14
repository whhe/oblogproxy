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

#include "cluster.h"

namespace oceanbase::logproxy {

ClusterProtocol* ClusterProtocolFactory::create(
    ClusterConfig* cluster_config, ProtocolMode mode, TaskExecutorCb& task_executor_cb, InstanceFailoverCb& recover_cb)
{
  switch (mode) {
    case DATABASE: {
      return new MetaDBProtocol(cluster_config, task_executor_cb, recover_cb);
    }
    case GOSSIP: {
      break;
    }
    case STANDALONE: {
      return new StandAloneProtocol(cluster_config, task_executor_cb, recover_cb);
    }
  }
  return nullptr;
}

ClusterProtocolFactory::~ClusterProtocolFactory() = default;
}  // namespace oceanbase::logproxy
