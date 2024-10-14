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
#include "gtest/gtest.h"

#include "common.h"
#include "cluster/instance_meta.h"

using namespace oceanbase::binlog;
using namespace oceanbase::logproxy;

TEST(InstanceMeta, init)
{
  Config s_config = Config::instance();

  InstanceMeta meta(s_config, "name", "cluster", "tenant");
  std::map<std::string, std::string> instance_options;
  instance_options.emplace("initial_ob_txn_id", "1002_2199679");
  instance_options.emplace("initial_ob_txn_gtid_seq", "23");
  instance_options.emplace("start_timestamp", "1701325833");
  instance_options.emplace("cluster_url", "http:xxxxx");
  meta.init(instance_options);
  ASSERT_STREQ(meta.cluster().c_str(), "cluster");
  OMS_INFO("InstanceMeta: {}", meta.serialize_to_json());
}
