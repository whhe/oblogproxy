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

#include "task.h"

#include "timer.h"
#include "guard.hpp"
#include "cluster_protocol.h"
#include "fs_util.h"

namespace oceanbase::logproxy {
std::string task_status_str(uint16_t status)
{
  static const char* _s_task_status_names[] = {"Waiting", "Executing", "Completed", "Failed"};
  if (status >= TaskStatus::WAITING && status <= TaskStatus::FAILED) {
    return _s_task_status_names[status];
  }
  return "Undefined";
}

std::string task_type_str(uint16_t type)
{
  static const char* _s_task_status_names[] = {
      "CreateInstance", "DropInstance", "StopInstance", "StartInstance", "RecoverInstance"};
  if (type >= TaskType::CREATE_INSTANCE && type <= TaskType::RECOVER_INSTANCE) {
    return _s_task_status_names[type];
  }
  return "Undefined";
}

void ScheduledPullTask::run()
{
  Timer timer;
  std::string local_node_id = _node->get_node_info().id();
  OMS_INFO("Begin to pull tasks of node [{}] every {} us", local_node_id, _interval_us);
  while (is_run()) {
    std::vector<Task> tasks;
    int ret = _node->fetch_unfinished_tasks(local_node_id, tasks);
    if (ret != OMS_OK) {
      OMS_ERROR("Failed to fetch tasks of node: {}, and try again !!!", _node->get_node_info().identifier());
    } else {
      for (auto task : tasks) {
        OMS_INFO("Begin to execute task: {}", task.serialize_to_json());
        _node->get_task_executor_cb()(task);
      }
    }

    timer.sleep(_interval_us);
  }
  OMS_ERROR("!!! Exit the thread that pulls tasks of node [{}] periodically.", local_node_id);
}

ScheduledPullTask::ScheduledPullTask(ClusterProtocol* node) : _node(node)
{
  _interval_us = node->get_cluster_config()->task_interval_us();
}

int LocalTaskManager::storage_task(const Task& task)
{
  std::lock_guard<std::mutex> op_lock(_op_mutex);
  FILE* fp = FsUtil::fopen_binary(_task_file_name);
  if (fp == nullptr) {
    OMS_ERROR("Failed to open file: {}", _task_file_name);
    return OMS_FAILED;
  }

  std::string content = task.serialize_to_json();
  logproxy::FsUtil::append_file(fp, (unsigned char*)content.c_str(), content.size());
  logproxy::FsUtil::fclose_binary(fp);
  OMS_INFO("Add task file: {}, value: ", _task_file_name, content);
  return OMS_OK;
}

int LocalTaskManager::fetch_tasks(std::vector<Task*> tasks)
{
  std::ifstream ifs(_task_file_name);
  if (!ifs.good()) {
    OMS_ERROR("Failed to open state file: {}, error: {}", _task_file_name, logproxy::system_err(errno));
    return OMS_FAILED;
  }

  for (std::string line; std::getline(ifs, line);) {
    auto* task = new Task();
    task->deserialize_from_json(line);
    tasks.push_back(task);
  }
  return OMS_OK;
}

int LocalTaskManager::update_task(const Task& task)
{
  std::lock_guard<std::mutex> op_lock(_op_mutex);
  std::vector<Task*> tasks;
  defer(logproxy::release_vector(tasks));
  fetch_tasks(tasks);
  std::string temp = _task_file_name + "_" + std::to_string(getpid()) + ".tmp";
  FILE* fptmp = logproxy::FsUtil::fopen_binary(temp, "a+");
  if (fptmp == nullptr) {
    OMS_STREAM_ERROR << "Failed to open file:" << temp;
    return OMS_FAILED;
  }

  for (Task* task_entry : tasks) {
    if (task_entry->task_id() == task.task_id()) {
      task_entry->deserialize_from_json(task.serialize_to_json());
    }
    // Write to tmp file
    std::string value = task_entry->serialize_to_json();
    logproxy::FsUtil::append_file(fptmp, (unsigned char*)value.c_str(), value.size());
  }

  logproxy::FsUtil::fclose_binary(fptmp);
  std::error_code error_code;

  fs::rename(_task_file_name, _task_file_name + "-bak", error_code);
  if (error_code) {
    OMS_STREAM_ERROR << "Failed to rename file:" << _task_file_name << " by " << error_code.message() << temp;
    return OMS_FAILED;
  }

  fs::rename(temp, _task_file_name, error_code);
  if (error_code) {
    OMS_STREAM_ERROR << "Failed to rename file:" << _task_file_name << " by " << error_code.message() << temp;
    return OMS_FAILED;
  }
  if (fs::exists(_task_file_name + "-bak")) {
    fs::remove(_task_file_name + "-bak");
  }
  return OMS_OK;
}

}  // namespace oceanbase::logproxy
