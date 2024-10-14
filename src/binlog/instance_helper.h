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

#include "instance_meta.h"

namespace oceanbase::binlog {

class BinlogInstanceHelper {
public:
  BinlogInstanceHelper() = delete;

private:
  /**
   * Generate server_id according to rule: OBI IP last address segment + current microsecond
   */
  static uint32_t gen_server_id(std::string& node_ip)
  {
    size_t last_dot_pos = node_ip.find_last_of('.');
    uint32_t id_prefix = 0;
    if (last_dot_pos != std::string::npos) {
      std::string last_segm = node_ip.substr(last_dot_pos + 1);
      id_prefix = std::atoi(last_segm.c_str()) * 1000000;
    }
    struct timeval tm {};
    gettimeofday(&tm, nullptr);
    return id_prefix + tm.tv_usec;
  }

  static std::string gen_server_uuid()
  {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 15);

    const char* v = "0123456789abcdef";
    std::stringstream ss;
    for (int i = 0; i < 8; i++) {
      ss << v[dist(gen)];
    }
    ss << '-';

    for (int i = 0; i < 4; i++) {
      ss << v[dist(gen)];
    }
    ss << '-';

    for (int i = 0; i < 4; i++) {
      ss << v[dist(gen)];
    }
    ss << '-';

    for (int i = 0; i < 4; i++) {
      ss << v[dist(gen)];
    }
    ss << '-';

    for (int i = 0; i < 12; i++) {
      ss << v[dist(gen)];
    }
    return ss.str();
  }

  static bool is_instance_unique_config(
      const vector<BinlogEntry>& instance_vec, std::function<bool(BinlogEntry)> compare_func)
  {
    for (const auto& instance : instance_vec) {
      if (compare_func(instance)) {
        return false;
      }
    }
    return true;
  }

  /* [a-z][a-z0-9]{9} */
  static std::string gen_random_name()
  {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 35);

    std::string str;
    str += (dis(gen) % 26) + 'a';
    for (int i = 0; i < 9; i++) {
      char ch;
      int random_num = dis(gen);
      if (random_num < 10) {
        ch = random_num + '0';
      } else {
        ch = (random_num - 10) + 'a';
      }
      str += ch;
    }
    return str;
  }

public:
  static void check_or_config_instance_server_options(
      const vector<BinlogEntry>& instance_vec, std::string node_ip, InstanceMeta* meta)
  {
    if (0 == meta->binlog_config()->server_id()) {
      config_instance_server_id(instance_vec, node_ip, meta);
    }
    if (meta->binlog_config()->server_uuid().empty()) {
      config_instance_server_uuid(instance_vec, meta);
    }
  }

  static void config_instance_server_options(
      const vector<BinlogEntry>& instance_vec, std::string node_ip, InstanceMeta* meta)
  {
    config_instance_server_id(instance_vec, node_ip, meta);
    config_instance_server_uuid(instance_vec, meta);
  }

  static void config_instance_server_id(
      const vector<BinlogEntry>& instance_vec, std::string& node_ip, InstanceMeta* meta)
  {
    uint32_t server_id;
    do {
      server_id = gen_server_id(node_ip);
    } while (!is_instance_unique_config(instance_vec,
        [&](BinlogEntry instance) -> bool { return instance.config()->binlog_config()->server_id() == server_id; }));
    meta->binlog_config()->set_server_id(server_id);
  }

  static void config_instance_server_uuid(const vector<BinlogEntry>& instance_vec, InstanceMeta* meta)
  {
    std::string server_uuid;
    do {
      server_uuid = gen_server_uuid();
    } while (!is_instance_unique_config(instance_vec, [&](BinlogEntry instance) -> bool {
      return instance.config()->binlog_config()->server_uuid() == server_uuid;
    }));
    meta->binlog_config()->set_server_uuid(server_uuid);
  }

  static void config_instance_name(const vector<BinlogEntry>& instance_vec, InstanceMeta* meta)
  {
    std::string instance_name;
    do {
      instance_name = gen_random_name();
    } while (!is_instance_unique_config(instance_vec, [&](BinlogEntry instance) -> bool {
      return strcmp(instance_name.c_str(), instance.instance_name().c_str()) == 0;
    }));
    meta->set_instance_name(instance_name);
  }
};

}  // namespace oceanbase::binlog