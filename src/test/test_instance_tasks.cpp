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
#include "instance_tasks.h"

using namespace oceanbase::binlog;
using namespace oceanbase::logproxy;

TEST(InstanceAsynTasks, init_starting_point)
{
  std::vector<BinlogEntry*> instance_entries;
  BinlogEntry instance;
  instance.set_config(new InstanceMeta(true));
  int ret = InstanceAsynTasks::init_starting_point(instance_entries, instance);
  ASSERT_EQ(ret, OMS_OK);
}