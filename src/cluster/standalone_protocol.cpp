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

#include "standalone_protocol.h"

namespace oceanbase::logproxy {

StandAloneProtocol::StandAloneProtocol(
    ClusterConfig* cluster_config, TaskExecutorCb& task_executor_cb, InstanceFailoverCb& recover_cb)
    : ClusterProtocol(cluster_config, task_executor_cb, recover_cb)
{}

StandAloneProtocol::~StandAloneProtocol()
{
  _pull_task.stop();
  _pull_task.join();
}

int StandAloneProtocol::register_node()
{
  _pull_task.start();
  OMS_INFO("Standalone node startup: {}", get_node_info().serialize_to_json());
  return OMS_OK;
}

int StandAloneProtocol::unregister_node()
{
  return OMS_OK;
}

int StandAloneProtocol::publish_task(Task& task)
{
  /*!
   * Submit directly to the local thread pool for execution
   */

  return _local_task_manager.storage_task(task);
}

int StandAloneProtocol::fetch_task(const Task& task, vector<Task*>& tasks)
{
  return 0;
}

int StandAloneProtocol::fetch_unfinished_tasks(const string& node_id, vector<Task>& tasks)
{
  return 0;
}

int StandAloneProtocol::fetch_recent_offline_task(const string& instance_name, Task& task)
{
  return 0;
}

int StandAloneProtocol::query_all_nodes(vector<Node*>& nodes)
{
  return 0;
}

int StandAloneProtocol::query_nodes(vector<Node*>& nodes, const Node& condition)
{
  return 0;
}

int StandAloneProtocol::query_nodes(vector<Node*>& nodes, const std::set<State> node_states)
{
  return 0;
}

int StandAloneProtocol::query_nodes(set<std::string>& node_ids, map<std::string, Node>& nodes)
{
  return 0;
}

int StandAloneProtocol::query_instances(vector<BinlogEntry*>& instances, const BinlogEntry& condition)
{
  return 0;
}

int StandAloneProtocol::query_instances(vector<BinlogEntry*>& instances, const set<std::string>& instance_names)
{
  return 0;
}

int StandAloneProtocol::query_instances(vector<BinlogEntry*>& instances, const set<InstanceState>& instance_states)
{
  return 0;
}

int StandAloneProtocol::update_instances(const BinlogEntry& instance, const BinlogEntry& condition)
{
  return 0;
}

int StandAloneProtocol::add_instance(const BinlogEntry& instance)
{
  std::lock_guard<std::mutex> op_lock(_op_mutex);
  FILE* fp = FsUtil::fopen_binary(_state_filename);
  if (fp == nullptr) {
    OMS_ERROR("Failed to open file: {}", _state_filename);
    return OMS_FAILED;
  }

  std::string content = instance.serialize_to_json();
  logproxy::FsUtil::append_file(fp, (unsigned char*)content.c_str(), content.size());
  logproxy::FsUtil::fclose_binary(fp);
  OMS_INFO("Add state machine file: {}, value: ", _state_filename, content);
  return OMS_OK;
}

int StandAloneProtocol::update_task(Task& task)
{
  return _local_task_manager.update_task(task);
}

int StandAloneProtocol::update_tasks(Task& task, const Task& condition)
{
  return 0;
}

int StandAloneProtocol::update_instance(const BinlogEntry& instance)
{
  std::lock_guard<std::mutex> op_lock(_op_mutex);
  std::vector<BinlogEntry*> state_machines;
  defer(logproxy::release_vector(state_machines));
  // TODO Query all instances
  query_all_instances(state_machines);
  std::string temp = _state_filename + "_" + std::to_string(getpid()) + ".tmp";
  FILE* fptmp = logproxy::FsUtil::fopen_binary(temp, "a+");
  if (fptmp == nullptr) {
    OMS_STREAM_ERROR << "Failed to open file:" << temp;
    return OMS_FAILED;
  }

  for (BinlogEntry* p_state_machine : state_machines) {
    if (p_state_machine->equal(instance)) {
      p_state_machine->deserialize_from_json(instance.serialize_to_json());
    }
    // Write to tmp file
    std::string value = p_state_machine->serialize_to_json();
    logproxy::FsUtil::append_file(fptmp, (unsigned char*)value.c_str(), value.size());
  }

  logproxy::FsUtil::fclose_binary(fptmp);
  std::error_code error_code;

  fs::rename(_state_filename, _state_filename + "-bak", error_code);
  if (error_code) {
    OMS_STREAM_ERROR << "Failed to rename file:" << _state_filename << " by " << error_code.message() << temp;
    return OMS_FAILED;
  }

  fs::rename(temp, _state_filename, error_code);
  if (error_code) {
    OMS_STREAM_ERROR << "Failed to rename file:" << _state_filename << " by " << error_code.message() << temp;
    return OMS_FAILED;
  }
  if (fs::exists(_state_filename + "-bak")) {
    fs::remove(_state_filename + "-bak");
  }
  return OMS_OK;
}

int StandAloneProtocol::query_init_instance_config(ConfigTemplate& config, const std::string& key_name,
    const std::string& group, const std::string& cluster, const std::string& tenant, const std::string& instance)
{
  return OMS_OK;
}

int StandAloneProtocol::query_instance_configs(std::vector<ConfigTemplate*>& configs, const std::string& group,
    const std::string& cluster, const std::string& tenant, const std::string& instance)
{
  return OMS_OK;
}

int StandAloneProtocol::query_configs_by_granularity(
    std::map<std::string, std::string>& configs, Granularity granularity, const std::string& scope)
{
  return OMS_OK;
}

int StandAloneProtocol::query_instance_configs(std::map<std::string, std::string>& configs, const std::string& group,
    const std::string& cluster, const std::string& tenant, const std::string& instance)
{
  return OMS_OK;
}

int StandAloneProtocol::replace_config(
    std::string key, std::string value, Granularity granularity, const std::string& scope)
{
  return OMS_OK;
}

int StandAloneProtocol::delete_config(std::string key, Granularity granularity, const std::string& scope)
{
  return OMS_OK;
}

int StandAloneProtocol::query_all_instances(vector<BinlogEntry*>& instances)
{
  std::ifstream ifs(_state_filename);
  if (!ifs.good()) {
    OMS_ERROR("Failed to open state file: {}, error: {}", _state_filename, logproxy::system_err(errno));
    return OMS_FAILED;
  }

  for (std::string line; std::getline(ifs, line);) {
    auto* instance = new BinlogEntry();
    instance->deserialize_from_json(line);
    instances.push_back(instance);
  }
  return OMS_OK;
}

int StandAloneProtocol::query_instances(vector<BinlogEntry*>& instances, std::string slot, SlotType type)
{
  return 0;
}

int StandAloneProtocol::query_task_by_id(const string& task_id, Task& task)
{
  return 0;
}

int StandAloneProtocol::query_instance_by_name(const string& instance_name, BinlogEntry& entry)
{
  return 0;
}

int StandAloneProtocol::query_master_instance(
    const std::string& cluster, const std::string& tenant, std::string& instance_name)
{
  return OMS_OK;
}

int StandAloneProtocol::determine_primary_instance(
    const std::string& cluster, const std::string& tenant, const std::string& instance_name, std::string& master)
{
  return OMS_OK;
}

int StandAloneProtocol::reset_master_instance(
    const std::string& cluster, const std::string& tenant, const std::string& instance_name)
{
  return OMS_OK;
}

int StandAloneProtocol::query_node_by_id(const string& node_id, Node& node)
{
  return 0;
}

int StandAloneProtocol::boundary_gtid_seq(
    const string& cluster, const string& tenant, pair<InstanceGtidSeq, InstanceGtidSeq>& boundary_pair)
{
  return 0;
}

int StandAloneProtocol::boundary_gtid_seq(const string& instance, pair<InstanceGtidSeq, InstanceGtidSeq>& boundary_pair)
{
  return 0;
}

int StandAloneProtocol::find_gtid_seq_by_timestamp(
    const string& cluster, const string& tenant, uint64_t timestamp, InstanceGtidSeq& gtid_seq)
{
  return 0;
}

int StandAloneProtocol::find_gtid_seq_by_timestamp(
    const string& instance, uint64_t timestamp, InstanceGtidSeq& gtid_seq)
{
  return 0;
}

int StandAloneProtocol::remove_instance_gtid_seq(const string& instance)
{
  return 0;
}

int StandAloneProtocol::remove_tenant_gtid_seq(const string& cluster, const string& tenant)
{
  return 0;
}

int StandAloneProtocol::get_gtid_seq_checkpoint(vector<InstanceGtidSeq>& instance_gtid_seq_vec)
{
  return 0;
}

int StandAloneProtocol::drop_instances(std::vector<Task>& task_vec)
{
  return 0;
}

int StandAloneProtocol::add_instance(const BinlogEntry& instance, Task& task)
{
  return 0;
}

int StandAloneProtocol::query_tenant_surviving_instances(
    string& cluster, string& tenant, vector<BinlogEntry>& instances)
{
  return 0;
}

int StandAloneProtocol::query_user_by_name(const string& name, User& user)
{
  return 0;
}

int StandAloneProtocol::alter_user_password(const User& user, const string& new_password)
{
  return 0;
}

int StandAloneProtocol::query_sys_user(User& user)
{
  return 0;
}

}  // namespace oceanbase::logproxy
