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

#include <utility>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <mutex>

#include "thread.h"
#include "env.h"
#include "fs_util.h"
#include "cluster/node.h"
#include "config.h"
#include "obcdc_config.h"
#include "sql/statements.h"
#include "metric/prometheus.h"

#define ERR_ADDR_INUSE -98

using namespace oceanbase::logproxy;

namespace oceanbase::binlog {

enum ObiInternalState { OK, EXIT, BIND_ERROR, MAY_HANG_ERROR };

class InstanceAsynTasks {
public:
  static int create_binlog_instance(const std::string& cluster, const std::string& tenant,
      const std::string& instance_name, const std::string& node_id, bool recover);

  static int start_binlog_instance(
      const std::string& instance_name, const hsql::InstanceFlag&, const std::string& task_id);

  static int stop_binlog_instance(
      const std::string& instance_name, const hsql::InstanceFlag&, const std::string& task_id);

  static int drop_binlog_instance(const std::string& status_config, const std::string& task_id, bool drop_all = false);

  static int invoke_fork(const std::string& work_path, const std::string& tcp_port, int& obi_pid);

  static int init_starting_point(std::vector<BinlogEntry*>& instance_entries, BinlogEntry& instance);

  static bool is_work_path_occupied(const std::string& work_path, bool& is_existed);

private:
  static int start_binlog_instance_inner(BinlogEntry& instance);

  static void kill_binlog_instance(const BinlogEntry& instance);

  static int serialize_configs(InstanceMeta* meta, const string& config_file);

  static void try_elect_master(BinlogEntry& instance);

  static int correct_initial_gtid(
      uint64_t start_timestamp, std::vector<BinlogEntry*>& instance_entries, BinlogEntry& instance);

  static void check_or_config_server_option(BinlogEntry& instance);

  static void drop_remote_failover_instance_logically(std::string& offline_instalce);

  static void mark_instance_failover(bool recover, Task task);
};

class TaskHandler {
public:
  virtual void execute(Task& task, BinlogEntry& instance) = 0;

  virtual void interrupt_recover(Task& task, BinlogEntry& instance) = 0;

public:
  virtual bool execNow(Task& task)
  {
    return true;
  }

protected:
  static void recover_starting_instance(Task& task, BinlogEntry& instance);
};

class CreateTaskHandler : public TaskHandler {
  OMS_SINGLETON(CreateTaskHandler);

public:
  void execute(Task& task, BinlogEntry& instance) override;

  void interrupt_recover(Task& task, BinlogEntry& instance) override;
};

class DropTaskHandler : public TaskHandler {
  OMS_SINGLETON(DropTaskHandler);

public:
  void execute(Task& task, BinlogEntry& instance) override;

  void interrupt_recover(Task& task, BinlogEntry& instance) override;

public:
  bool execNow(Task& task) override;
};

class StopTaskHandler : public TaskHandler {
  OMS_SINGLETON(StopTaskHandler);

public:
  void execute(Task& task, BinlogEntry& instance) override;

  void interrupt_recover(Task& task, BinlogEntry& instance) override;
};

class StartTaskHandler : public TaskHandler {
  OMS_SINGLETON(StartTaskHandler);

public:
  void execute(Task& task, BinlogEntry& instance) override;

  void interrupt_recover(Task& task, BinlogEntry& instance) override;
};

class RecoverTaskHandler : public TaskHandler {
  OMS_SINGLETON(RecoverTaskHandler);

public:
  void execute(Task& task, BinlogEntry& instance) override;

  void interrupt_recover(Task& task, BinlogEntry& instance) override;
};

class TaskCallback {
public:
  static void task_executor(Task& task);

private:
  static void handle_waiting_or_failed_task(Task& task);

  static void handle_executing_task(Task& task);
};

class InstanceFailoverCallback {
public:
  static void instance_failover(const BinlogEntry& offline_instance);

private:
  static void recover_offline_instance(const BinlogEntry& instance);
};

bool is_master_instance(const BinlogEntry& offline_instance);

void master_election(const std::string& cluster, const std::string& tenant, const std::string& prev_master_instance,
    uint64_t min_dump_checkpoint, bool only_reset = false);

}  // namespace oceanbase::binlog
