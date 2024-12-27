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

#include "tcp_port_pool.h"
#include "common.h"
#include "env.h"

using namespace oceanbase::binlog;
TEST(TcpPortPool, init_pool)
{
  env_init(16, 16, 8100, 512,512);

  // case 1: failed
  TcpPortPool port_pool_0(8100, 0);
  ASSERT_EQ(port_pool_0.init(), OMS_FAILED);

  TcpPortPool port_pool_1024(8100, 2000);
  ASSERT_EQ(port_pool_1024.init(), OMS_FAILED);

  TcpPortPool port_pool_invalid_range(65535, 1024);
  ASSERT_EQ(port_pool_invalid_range.init(), OMS_FAILED);

  // case 2
  TcpPortPool port_pool(50000, 1024);
  ASSERT_EQ(port_pool.init(), OMS_OK);
  ASSERT_GE(1024, port_pool.available_port_count());

  env_deInit();
}

TEST(TcpPortPool, take_port)
{
  env_init(16, 16, 8100, 512,512);
  uint16_t port;

  // case 1
  TcpPortPool port_pool_normal(50000, 10);
  ASSERT_EQ(OMS_OK, port_pool_normal.init());
  size_t init_ports_count = port_pool_normal.available_port_count();
  int count1 = 0;
  while (true) {
    int ret = port_pool_normal.take_port(port, false);
    if (OMS_FAILED == ret) {
      break;
    }
    count1++;
  }
  ASSERT_GE(init_ports_count, count1);

  // case 2
  TcpPortPool port_pool_return(50000, 15);
  ASSERT_EQ(OMS_OK, port_pool_return.init());
  size_t init_ports_count2 = port_pool_return.available_port_count();
  size_t loop_times = init_ports_count2 + 5;
  while (loop_times--) {
    int ret = port_pool_return.take_port(port, false);
    ASSERT_EQ(OMS_OK, ret);
    port_pool_return.return_port(port);
  }
  ASSERT_EQ(init_ports_count2, port_pool_return.available_port_count());

  // case 3
  TcpPortPool port_pool_take_with_return(50000, 8);
  ASSERT_EQ(OMS_OK, port_pool_take_with_return.init());
  size_t init_ports_count3 = port_pool_take_with_return.available_port_count();
  int count3 = 0;
  while (true) {
    int ret = port_pool_take_with_return.take_port(port, false);
    if (OMS_FAILED == ret) {
      break;
    }
    count3++;
    if (count3 == 4 || count3 == 6) {
      port_pool_take_with_return.return_port(port - 2);
    }
  }
  ASSERT_EQ(init_ports_count3 + 2, count3);

  // case 4
  {
    TcpPortPool port_pool_with_all_ports_in_used(50000, 5);
    ASSERT_EQ(OMS_OK, port_pool_with_all_ports_in_used.init());
    size_t init_ports_count4 = port_pool_take_with_return.available_port_count();
    ASSERT_EQ(init_ports_count4, 5);
    for (int i = 0; i < init_ports_count4; i++) {
      port_pool_with_all_ports_in_used.mark_port(50000 + i, true);
    }
    uint16_t port2;
    int ret = port_pool_with_all_ports_in_used.take_port(port2, false);
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(port2, 50000);
  }
  env_deInit();
}