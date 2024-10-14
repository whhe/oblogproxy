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
#include "model.h"
#include "thread.h"
#include "node.h"
#include "instance_meta.h"

#define MAX_TASK_RETRY_TIMES 3
namespace oceanbase::logproxy {

#define LOCAL_TASK_FILE_DEFAULT "tasks"

enum TaskStatus { WAITING, EXECUTING, COMPLETED, FAILED, UNDEF = UINT16_MAX };

std::string task_status_str(uint16_t status);

enum TaskType {
  CREATE_INSTANCE,
  DROP_INSTANCE,
  STOP_INSTANCE,
  START_INSTANCE,
  RECOVER_INSTANCE,
  TASK_TYPE_UNDEF = UINT16_MAX,
};
std::string task_type_str(uint16_t type);

struct CreateInstanceTaskParam : Object {
  MODEL_DCL(CreateInstanceTaskParam);
  MODEL_DEF_STR(cluster, "");
  MODEL_DEF_STR(tenant, "");
  MODEL_DEF_STR(cluster_id, "");
  MODEL_DEF_STR(tenant_id, "");

  explicit CreateInstanceTaskParam(bool){};
};
MODEL_DEF(CreateInstanceTaskParam);

struct StopInstanceTaskParam : Object {
  MODEL_DCL(StopInstanceTaskParam);
  MODEL_DEF_UINT16(flag, 0);

  explicit StopInstanceTaskParam(bool){};
};
MODEL_DEF(StopInstanceTaskParam);

struct StartInstanceTaskParam : Object {
  MODEL_DCL(StartInstanceTaskParam);
  MODEL_DEF_UINT16(flag, 0);

  explicit StartInstanceTaskParam(bool){};
};
MODEL_DEF(StartInstanceTaskParam);

struct DropInstanceTaskParam : Object {
  MODEL_DCL(DropInstanceTaskParam);
  MODEL_DEF_BOOL(drop_all, false);
  MODEL_DEF_UINT32(expect_exec_time, 0);

  explicit DropInstanceTaskParam(bool){};
};
MODEL_DEF(DropInstanceTaskParam);

struct RecoverInstanceTaskParam : Object {
  MODEL_DCL(RecoverInstanceTaskParam);
  MODEL_DEF_STR(cluster, "");
  MODEL_DEF_STR(tenant, "");
  MODEL_DEF_STR(cluster_id, "");
  MODEL_DEF_STR(tenant_id, "");
  MODEL_DEF_STR(offline_instance, "");

  explicit RecoverInstanceTaskParam(bool){};
};
MODEL_DEF(RecoverInstanceTaskParam);

struct Task : public Object {
  MODEL_DCL(Task);
  MODEL_DEF_STR(task_id, "");
  MODEL_DEF_STR(instance_name, "");
  MODEL_DEF_UINT16(type, TASK_TYPE_UNDEF);
  MODEL_DEF_UINT16(status, UNDEF);
  MODEL_DEF_STR(execution_node, "");
  MODEL_DEF_UINT64(retry_count, 0);
  MODEL_DEF_UINT64(last_modify, 0);
  /*!
   * @brief Everything for each different task implementation needs to be recorded in task_param
   * The task param will record the details of what the task needs to do.
   */
  MODEL_DEF_OBJECT(task_param, Object);

public:
  explicit Task(bool initialize){};

  void init_create_task(Node& node, InstanceMeta& meta)
  {
    this->set_type(TaskType::CREATE_INSTANCE);
    this->set_status(TaskStatus::WAITING);
    this->set_execution_node(node.id());
    this->set_instance_name(meta.instance_name());
    auto* param = new CreateInstanceTaskParam();
    param->set_cluster(meta.cluster());
    param->set_tenant(meta.tenant());
    param->set_cluster_id(meta.cluster_id());
    param->set_tenant_id(meta.tenant_id());
    this->set_task_param(param);
  }

  void init_recover_task(Node& node, InstanceMeta& meta, std::string offline_instance)
  {
    this->set_type(TaskType::RECOVER_INSTANCE);
    this->set_status(TaskStatus::WAITING);
    this->set_execution_node(node.id());
    this->set_instance_name(meta.instance_name());
    auto* param = new RecoverInstanceTaskParam();
    param->set_cluster(meta.cluster());
    param->set_tenant(meta.tenant());
    param->set_cluster_id(meta.cluster_id());
    param->set_cluster_id(meta.tenant_id());
    param->set_offline_instance(offline_instance);
    this->set_task_param(param);
  }
};
MODEL_DEF(Task);

class ClusterProtocol;

class ScheduledPullTask : public Thread {
public:
  explicit ScheduledPullTask(ClusterProtocol* node);

private:
  void run() override;

  uint64_t _interval_us;
  ClusterProtocol* _node;
};

class LocalTaskManager {
public:
  int storage_task(const Task& task);

  int update_task(const Task& task);

  int fetch_tasks(std::vector<Task*> tasks);

private:
  std::mutex _op_mutex;
  std::string _task_file_name;
};

}  // namespace oceanbase::logproxy
