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
#include "common.h"
#include "model.h"

namespace oceanbase::logproxy {
struct ClusterConfig : public Object {
  MODEL_DCL(ClusterConfig);
  /*!
   * @brief database ip
   */
  MODEL_DEF_STR(database_ip, "");
  MODEL_DEF_UINT16(database_port, 0);
  /*!
   * @brief The database name corresponding to the meta database
   */
  MODEL_DEF_STR(database_name, "");
  // k1=v1 k2=v2
  MODEL_DEF_STR(database_properties, "");
  MODEL_DEF_INT(min_pool_size, 30);
  MODEL_DEF_STR(user, "");
  MODEL_DEF_STR(password, "");
  MODEL_DEF_UINT64(push_pull_interval_us, 1000000);
  MODEL_DEF_UINT64(task_interval_us, 1000000);
  MODEL_DEF_UINT64(metric_interval_us, 1000000);

  /*!
   * @brief node configuration related
   */
  MODEL_DEF_STR(node_ip, "");
  MODEL_DEF_UINT16(server_port, 2983);
  MODEL_DEF_STR(node_id, "");
  MODEL_DEF_STR(region, "");
  MODEL_DEF_STR(zone, "");
  MODEL_DEF_OBJECT(node_config, NodeConfig);
  ClusterConfig(bool)
  {}
};
MODEL_DEF(ClusterConfig);
}  // namespace oceanbase::logproxy
