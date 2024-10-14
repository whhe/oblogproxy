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

#include "binlog_state_machine.h"

#include <filesystem>
#include <utility>
#include <fstream>

#include "str.h"
#include "common.h"
#include "fs_util.h"

namespace oceanbase::binlog {

namespace fs = std::filesystem;

//
// StateMachine::StateMachine(std::string _cluster, std::string _tenant)
//{
//  this->_cluster = std::move(_cluster);
//  this->_tenant = std::move(_tenant);
//}
//
// const std::string& StateMachine::get_instance_name() const
//{
//  return _instance_name;
//}
//
// void StateMachine::set_instance_name(std::string instance_name)
//{
//  _instance_name = std::move(instance_name);
//}
//
// const std::string& StateMachine::get_cluster() const
//{
//  return _cluster;
//}
//
// void StateMachine::set_cluster(std::string cluster)
//{
//  _cluster = std::move(cluster);
//}
//
// const std::string& StateMachine::get_tenant() const
//{
//  return _tenant;
//}
//
// void StateMachine::set_tenant(std::string tenant)
//{
//  _tenant = std::move(tenant);
//}
//
// int StateMachine::get_pid() const
//{
//  return _pid;
//}
//
// void StateMachine::set_pid(int pid)
//{
//  _pid = pid;
//}
//
// uint16_t StateMachine::get_tcp_port() const
//{
//  return _tcp_port;
//}
//
// void StateMachine::set_tcp_port(uint16_t tcp_port)
//{
//  _tcp_port = tcp_port;
//}
//
// const std::string& StateMachine::get_work_path() const
//{
//  return _work_path;
//}
//
// void StateMachine::set_work_path(std::string work_path)
//{
//  _work_path = std::move(work_path);
//}
//
// InstanceState StateMachine::get_instance_state() const
//{
//  return _instance_state;
//}
//
// void StateMachine::set_instance_state(InstanceState instance_state)
//{
//  _instance_state = instance_state;
//}
//
// std::string StateMachine::get_config() const
//{
//  return _config;
//}
//
// void StateMachine::set_config(std::string config)
//{
//  _config = std::move(config);
//}
//
// std::string StateMachine::to_string()
//{
//  std::stringstream str;
//  str << _tcp_port << "\t" << _instance_name << "\t" << _cluster << "\t" << _tenant << "\t" << _pid << "\t"
//      << _work_path << "\t" << _instance_state << "\t" << _config << std::endl;
//  return str.str();
//}
//
// void StateMachine::parse(const std::string& content)
//{
//  std::vector<std::string> set;
//  logproxy::split(content, '\t', set);
//  if (set.size() == 8) {
//    this->set_tcp_port(std::atoi(set.at(0).c_str()));
//    this->set_instance_name(set.at(1));
//    this->set_cluster(set.at(2));
//    this->set_tenant(set.at(3));
//    this->set_pid(std::atoi(set.at(4).c_str()));
//    this->set_work_path(set.at(5));
//    this->set_instance_state(value_of(std::stol(set.at(6))));
//    this->set_config(set.at(7));
//    OMS_DEBUG("State machine information: {}", this->to_string());
//  }
//}
//
// StateMachine::StateMachine(uint16_t tcp_port, std::string instance_name, std::string cluster, std::string tenant,
//    int pid, std::string work_path, InstanceState instance_state, std::string config)
//    : _tcp_port(tcp_port),
//      _instance_name(std::move(instance_name)),
//      _cluster(std::move(cluster)),
//      _tenant(std::move(tenant)),
//      _pid(pid),
//      _work_path(std::move(work_path)),
//      _instance_state(instance_state),
//      _config(std::move(config))
//{}
//
// bool StateMachine::equal(StateMachine& state_machine) const
//{
//  return strcmp(get_unique_id().c_str(), state_machine.get_unique_id().c_str()) == 0;
//}
//
// void StateMachine::clone(StateMachine& state_machine)
//{
//  this->set_tcp_port(state_machine.get_tcp_port());
//  this->set_instance_name(state_machine.get_instance_name());
//  this->set_cluster(state_machine.get_cluster());
//  this->set_tenant(state_machine.get_tenant());
//  this->set_config(state_machine.get_config());
//  this->set_instance_state(state_machine.get_instance_state());
//  this->set_pid(state_machine.get_pid());
//  this->set_work_path(state_machine.get_work_path());
//}
//
// std::string StateMachine::get_unique_id() const
//{
//  return get_cluster() + "_" + get_tenant() + "_" + get_instance_name();
//}
//
// StateMachineManager::StateMachineManager()
//{
//  _state_filename = logproxy::Config::instance().binlog_log_bin_basename.val() + "/" + STATE_FILE_DEFAULT;
//}
//
// int StateMachineManager::add_state(StateMachine state_machine)
//{
//  std::lock_guard<std::mutex> op_lock(_op_mutex);
//  FILE* fp = logproxy::FsUtil::fopen_binary(_state_filename);
//  if (fp == nullptr) {
//    OMS_ERROR("Failed to open file: {}", _state_filename);
//    return OMS_FAILED;
//  }
//
//  logproxy::FsUtil::append_file(
//      fp, (unsigned char*)state_machine.to_string().c_str(), state_machine.to_string().size());
//  logproxy::FsUtil::fclose_binary(fp);
//  OMS_INFO("Add state machine file: {}, value: ", _state_filename, state_machine.to_string());
//  return OMS_OK;
//}
//
// int StateMachineManager::update_state(StateMachine state_machine)
//{
//  std::lock_guard<std::mutex> op_lock(_op_mutex);
//  std::vector<StateMachine*> state_machines;
//  fetch_state_vector_no_lock(state_machines);
//  std::string temp = _state_filename + "_" + std::to_string(getpid()) + ".tmp";
//  FILE* fptmp = logproxy::FsUtil::fopen_binary(temp, "a+");
//  if (fptmp == nullptr) {
//    OMS_STREAM_ERROR << "Failed to open file:" << temp;
//    logproxy::release_vector(state_machines);
//    return OMS_FAILED;
//  }
//
//  for (StateMachine* p_state_machine : state_machines) {
//    if (p_state_machine->equal(state_machine)) {
//      p_state_machine->clone(state_machine);
//    }
//    // Write to tmp file
//    std::string value = p_state_machine->to_string();
//    logproxy::FsUtil::append_file(fptmp, (unsigned char*)value.c_str(), value.size());
//  }
//
//  logproxy::FsUtil::fclose_binary(fptmp);
//  std::error_code error_code;
//
//  fs::rename(_state_filename, _state_filename + "-bak", error_code);
//  if (error_code) {
//    OMS_STREAM_ERROR << "Failed to rename file:" << _state_filename << " by " << error_code.message() << temp;
//    return OMS_FAILED;
//  }
//
//  fs::rename(temp, _state_filename, error_code);
//  if (error_code) {
//    OMS_STREAM_ERROR << "Failed to rename file:" << _state_filename << " by " << error_code.message() << temp;
//    return OMS_FAILED;
//  }
//  if (fs::exists(_state_filename + "-bak")) {
//    fs::remove(_state_filename + "-bak");
//  }
//  logproxy::release_vector(state_machines);
//  return OMS_OK;
//}
//
// int StateMachineManager::fetch_state_vector(std::vector<StateMachine*>& state_machines)
//{
//  std::lock_guard<std::mutex> op_lock(_op_mutex);
//  CompareFunc cmp_func = [&](const StateMachine& state) -> bool {
//    return !state.get_cluster().empty() && !state.get_config().empty();
//  };
//  return fetch_state_vector(cmp_func, state_machines);
//}
//
// int StateMachineManager::fetch_state_vector_no_lock(std::vector<StateMachine*>& state_machines)
//{
//  CompareFunc cmp_func = [&](const StateMachine& state) -> bool {
//    return !state.get_cluster().empty() && !state.get_config().empty();
//  };
//  return fetch_state_vector(cmp_func, state_machines);
//}
//
// int StateMachineManager::fetch_state_vector(
//    const std::string& cluster, const std::string& tenant, std::vector<StateMachine*>& state_machines)
//{
//  std::lock_guard<std::mutex> op_lock(_op_mutex);
//  CompareFunc cmp_func = [&](const StateMachine& state) -> bool {
//    return strcmp(cluster.c_str(), state.get_cluster().c_str()) == 0 &&
//           strcmp(tenant.c_str(), state.get_tenant().c_str()) == 0 && !state.get_config().empty();
//  };
//  return fetch_state_vector(cmp_func, state_machines);
//}
//
// int StateMachineManager::fetch_state_vector(
//    const std::vector<std::string>& instance_names, std::vector<StateMachine*>& state_machines)
//{
//  std::lock_guard<std::mutex> op_lock(_op_mutex);
//  CompareFunc cmp_func = [&](const StateMachine& state) -> bool {
//    return std::find(instance_names.begin(), instance_names.end(), state.get_instance_name()) != instance_names.end()
//    &&
//           !state.get_config().empty();
//  };
//  return fetch_state_vector(cmp_func, state_machines);
//}
//
// int StateMachineManager::fetch_state_vector(CompareFunc& cmp_func, std::vector<StateMachine*>& state_machines)
//{
//  std::ifstream ifs(_state_filename);
//  if (!ifs.good()) {
//    OMS_ERROR("Failed to open state file: {}, error: {}", _state_filename, logproxy::system_err(errno));
//    return OMS_FAILED;
//  }
//
//  for (std::string line; std::getline(ifs, line);) {
//    auto* state_machine = new StateMachine();
//    state_machine->parse(line);
//    if (cmp_func(*state_machine)) {
//      state_machines.emplace_back(state_machine);
//    } else {
//      delete (state_machine);
//    }
//  }
//  return OMS_OK;
//}
//
// int StateMachineManager::fetch_state(const std::string& cluster, const std::string& tenant,
//    const std::string& instance_name, StateMachine& state_machine)
//{
//  std::lock_guard<std::mutex> op_lock(_op_mutex);
//  CompareFunc cmp_func = [&](const StateMachine& state) -> bool {
//    return strcmp(cluster.c_str(), state.get_cluster().c_str()) == 0 &&
//           strcmp(tenant.c_str(), state.get_tenant().c_str()) == 0 &&
//           strcmp(instance_name.c_str(), state.get_instance_name().c_str()) == 0;
//  };
//  return fetch_state(cmp_func, state_machine);
//}
//
// int StateMachineManager::fetch_state(const std::string& instance_name, StateMachine& state_machine)
//{
//  std::lock_guard<std::mutex> op_lock(_op_mutex);
//  CompareFunc cmp_func = [&instance_name](const StateMachine& state) -> bool {
//    return strcmp(instance_name.c_str(), state.get_instance_name().c_str()) == 0;
//  };
//  return fetch_state(cmp_func, state_machine);
//}
//
// int StateMachineManager::fetch_state(CompareFunc& cmp_func, StateMachine& state_machine)
//{
//  std::ifstream ifs(_state_filename);
//  if (!ifs.good()) {
//    OMS_ERROR("Failed to open state file: {}, error: {}", _state_filename, logproxy::system_err(errno));
//    return OMS_FAILED;
//  }
//
//  for (std::string line; std::getline(ifs, line);) {
//    state_machine.parse(line);
//    if (cmp_func(state_machine)) {
//      return OMS_OK;
//    }
//  }
//  return OMS_FAILED;
//}

}  // namespace oceanbase::binlog
