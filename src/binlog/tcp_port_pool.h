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

#include <cstdint>
#include <bitset>
#include <mutex>

namespace oceanbase::binlog {

class TcpPortPool {
public:
  TcpPortPool(uint16_t start_tcp_port, uint16_t reserved_ports_num);

  ~TcpPortPool() = default;

  int init();

  void mark_port(uint16_t port, bool is_in_used);

  int take_port(uint16_t& port, bool test_and_reset = true);

  void return_port(uint16_t port);

  size_t available_port_count();

private:
  static bool is_port_in_used_actually(uint16_t port);

  void test_and_reset_ports_in_used();

  std::string get_available_ports_info();

private:
  uint16_t _start_tcp_port;
  uint16_t _reserved_ports_num;

  std::bitset<1024> _port_bits;

  // index of current pos in _port_bits
  uint16_t _bit_index = 0;

  std::mutex _op_mutex;
};

}  // namespace oceanbase::binlog
