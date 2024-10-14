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
#include <mutex>

#include "log.h"
#include "config.h"
#include "file_lock.h"

namespace oceanbase::binlog {

#define STATE_FILE_DEFAULT "state"

// class StateMachine {
// public:
//   StateMachine(uint16_t tcp_port, std::string instance_name, std::string cluster, std::string tenant, int pid,
//       std::string work_path, InstanceState converter_state, std::string config);
//
//   StateMachine(std::string _cluster, std::string _tenant);
//
//   StateMachine() = default;
//
//   const std::string& get_instance_name() const;
//
//   void set_instance_name(std::string instance_name);
//
//   const std::string& get_cluster() const;
//
//   void set_cluster(std::string cluster);
//
//   const std::string& get_tenant() const;
//
//   void set_tenant(std::string tenant);
//
//   int get_pid() const;
//
//   void set_pid(int pid);
//
//   uint16_t get_tcp_port() const;
//
//   void set_tcp_port(uint16_t tcp_port);
//
//   const std::string& get_work_path() const;
//
//   void set_work_path(std::string work_path);
//
//   InstanceState get_instance_state() const;
//
//   void set_instance_state(InstanceState instance_state);
//
//   std::string get_config() const;
//
//   void set_config(std::string config);
//
//   std::string to_string();
//
//   void parse(const std::string& content);
//
//   bool equal(StateMachine& state_machine) const;
//
//   void clone(StateMachine& state_machine);
//
//   std::string get_unique_id() const;
//
// private:
//   uint16_t _tcp_port = 0;
//   std::string _instance_name;
//   std::string _cluster;
//   std::string _tenant;
//   int _pid = 0;
//   std::string _work_path;
//   InstanceState _instance_state = InstanceState::INIT;
//   std::string _config;
// };
//
// typedef std::function<int(StateMachine)> CompareFunc;
//
// class StateMachineManager {
// public:
//   StateMachineManager();
//   ~StateMachineManager() = default;
//
// public:
//   int add_state(StateMachine state_machine);
//
//   int update_state(StateMachine state_machine);
//
//   int fetch_state(const std::string& cluster, const std::string& tenant, const std::string& instance_name,
//       StateMachine& state_machine);
//
//   int fetch_state(const std::string& instance_name, StateMachine& state_machine);
//
//   int fetch_state_vector(std::vector<StateMachine*>& state_machines);
//
//   int fetch_state_vector_no_lock(std::vector<StateMachine*>& state_machines);
//
//   int fetch_state_vector(
//       const std::string& cluster, const std::string& tenant, std::vector<StateMachine*>& state_machines);
//
//   int fetch_state_vector(const std::vector<std::string>& instance_names, std::vector<StateMachine*>& state_machines);
//
// private:
//   int fetch_state(CompareFunc& cmp_func, StateMachine& state_machine);
//
//   int fetch_state_vector(CompareFunc& cmp_func, std::vector<StateMachine*>& state_machines);
//
// private:
//   std::mutex _op_mutex;
//   std::string _state_filename;
// };

}  // namespace oceanbase::binlog
