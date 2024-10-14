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

#include "tcp_port_pool.h"

#include <vector>
#include <sys/socket.h>

#include "env.h"
#include "log.h"
#include "common.h"

namespace oceanbase::binlog {

TcpPortPool::TcpPortPool(uint16_t start_tcp_port, uint16_t reserved_ports_num)
    : _start_tcp_port(start_tcp_port), _reserved_ports_num(reserved_ports_num)
{}

int TcpPortPool::init()
{
  if (_reserved_ports_num <= 0 || _reserved_ports_num > 1024 || _start_tcp_port + _reserved_ports_num > 65535) {
    OMS_ERROR("Invalid port range, start_tcp_port: {}, reserved_ports_num:{}", _start_tcp_port, _reserved_ports_num);
    return OMS_FAILED;
  }

  // set ports int the range of [_reserved_ports_num, 1024) to be unavailable
  for (int i = _reserved_ports_num; i < _port_bits.size(); i++) {
    _port_bits.set(i);
  }

  // set ports in used as unavailable
  std::vector<BinlogEntry*> instance_entries;
  defer(release_vector(instance_entries));
  BinlogEntry condition;
  condition.set_node_id(g_cluster->get_node_info().id());
  binlog::g_cluster->query_instances(instance_entries, condition);
  for (const BinlogEntry* entry : instance_entries) {
    if (entry->state() == InstanceState::INIT || entry->state() == InstanceState::STARTING ||
        entry->state() == InstanceState::RUNNING || entry->state() == InstanceState::OFFLINE) {
      uint16_t port_in_used = entry->port();
      mark_port(port_in_used, true);
    }
  }

  OMS_INFO("Successfully init tcp port pool, available ports(total: {}): {}",
      (1024 - _port_bits.count()),
      get_available_ports_info());
  return OMS_OK;
}

int TcpPortPool::take_port(uint16_t& port, bool test_and_reset)
{
  std::lock_guard<std::mutex> op_lock(_op_mutex);
  while (!_port_bits.all()) {
    while (_bit_index < _reserved_ports_num) {
      if (!_port_bits.test(_bit_index)) {
        _port_bits.set(_bit_index);
        port = _start_tcp_port + _bit_index;
        if (!is_port_in_used_actually(port)) {
          return OMS_OK;
        }
      }

      _bit_index++;
    }

    if (test_and_reset) {
      test_and_reset_ports_in_used();
    }
    OMS_WARN("Only {} ports left, available ports: {}", (1024 - _port_bits.count()), get_available_ports_info());
    _bit_index = 0;
  }

  OMS_ERROR("No port available to be taken, and the reserved ports num: {}", _reserved_ports_num);
  return OMS_FAILED;
}

void TcpPortPool::return_port(uint16_t port)
{
  mark_port(port, false);
}

void TcpPortPool::mark_port(uint16_t port, bool is_in_used)
{
  std::lock_guard<std::mutex> op_lock(_op_mutex);
  uint16_t index = port - _start_tcp_port;
  if (index >= 0 && index < 1024) {
    _port_bits.set(index, is_in_used);
  }
}

bool TcpPortPool::is_port_in_used_actually(uint16_t port)
{
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (0 > fd) {
    OMS_WARN("Failed to create socket fd, error: {}", logproxy::system_err(errno));
    return true;
  }

  struct sockaddr_in addr_in {};
  addr_in.sin_family = AF_INET;
  addr_in.sin_port = htons(port);
  addr_in.sin_addr.s_addr = htonl(INADDR_ANY);
  bool is_in_used = false;
  if (bind(fd, (struct sockaddr*)(&addr_in), sizeof(sockaddr_in)) < 0) {
    is_in_used = true;
  }

  close(fd);
  return is_in_used;
}

std::string TcpPortPool::get_available_ports_info()
{
  std::string available_ports;
  for (uint16_t i = 0; i < _reserved_ports_num; i++) {
    if (!_port_bits.test(i)) {
      available_ports.append(to_string(_start_tcp_port + i) + std::string("|"));
    }
  }
  return available_ports;
}

size_t TcpPortPool::available_port_count()
{
  return (1024 - _port_bits.count());
}

void TcpPortPool::test_and_reset_ports_in_used()
{
  for (uint16_t i = 0; i < _reserved_ports_num; i++) {
    if (_port_bits.test(i) && !is_port_in_used_actually(_start_tcp_port + i)) {
      _port_bits.set(i, false);
    }
  }

  // set ports in used as unavailable
  std::vector<BinlogEntry*> instance_entries;
  defer(release_vector(instance_entries));
  BinlogEntry condition;
  condition.set_node_id(g_cluster->get_node_info().id());
  binlog::g_cluster->query_instances(instance_entries, condition);
  for (const BinlogEntry* entry : instance_entries) {
    if (entry->state() == InstanceState::OFFLINE) {
      mark_port(entry->port(), true);
    }
  }
}

}  // namespace oceanbase::binlog
