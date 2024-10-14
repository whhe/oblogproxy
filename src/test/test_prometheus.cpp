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
#include "metric/prometheus.h"

#include <thread>
#include <string>
#include <iostream>
#include <timer.h>

#include "gtest/gtest.h"
#include "log.h"

#include <communication/io.h>

using namespace oceanbase::logproxy;

TEST(PrometheusExposer, PrometheusExposer)
{
  init_prometheus(2988);
  OMS_INFO("prometheus exposer Starting");
  Timer timer;
  std::random_device rd;
  std::mt19937 gen(rd());

  std::uniform_int_distribution<int> dis(1, 100);
  double random_num = 0.0;
  while (timer.elapsed() < 1000000000) {
    // OMS_INFO("mark binlog node resource");
    random_num = dis(gen);
    PrometheusExposer::mark_binlog_node_resource("node1", "127.0.0.1", random_num, BINLOG_CPU_COUNT_TYPE);
    random_num = dis(gen);
    PrometheusExposer::mark_binlog_node_resource("node2", "127.0.0.3", random_num, BINLOG_CPU_COUNT_TYPE);
    timer.sleep(10000);
  }
}

TEST(PrometheusExposer, port_in_used)
{
  OMS_INFO("port in use :{}", port_in_use(12983));
}
