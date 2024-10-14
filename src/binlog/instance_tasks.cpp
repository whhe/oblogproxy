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

#include "instance_tasks.h"

#include "instance_meta.h"
#include "binlog_state_machine.h"
#include "binlog_index.h"
#include "instance_helper.h"

namespace oceanbase::binlog {

static std::map<TaskType, TaskHandler*> _s_task_handlers = {{TaskType::CREATE_INSTANCE, &CreateTaskHandler::instance()},
    {TaskType::DROP_INSTANCE, &DropTaskHandler::instance()},
    {TaskType::START_INSTANCE, &StartTaskHandler::instance()},
    {TaskType::STOP_INSTANCE, &StopTaskHandler::instance()},
    {TaskType::RECOVER_INSTANCE, &RecoverTaskHandler::instance()}};

std::function<void(int)> cancel_handler;

void signal_handler(int signal)
{
  cancel_handler(signal);
}

std::string get_instance_pid_file(const std::string& instance_work_path)
{
  return instance_work_path + string("/") + string(INSTANCE_PID_FILE);
}

std::string to_string(ObiInternalState state)
{
  std::string _internal_state_names[] = {"OK", "EXIT", "BIND_ERROR", "MAY_HANG_ERROR"};
  if (static_cast<uint8_t>(state) >= static_cast<uint8_t>(ObiInternalState::OK) &&
      static_cast<uint8_t>(state) <= static_cast<uint8_t>(ObiInternalState::MAY_HANG_ERROR)) {
    return _internal_state_names[static_cast<size_t>(state)];
  }
  return "UNKNOWN";
}

int InstanceAsynTasks::invoke_fork(const std::string& instance_work_path, const std::string& tcp_port, int& obi_pid)
{
  std::string instance_pid_file = get_instance_pid_file(instance_work_path);
  if (FsUtil::exist(instance_pid_file)) {
    FsUtil::remove(instance_pid_file, false);  // todo may be error
  }

  int pid = fork();
  if (pid == -1) {
    OMS_ERROR("Failed to fork, error: {}", system_err(errno));
    return OMS_FAILED;
  }

  if (pid == 0) {
    // children process
    // 1. close fds
    for (int i = 0; i < sysconf(_SC_OPEN_MAX); i++) {
      if (i != STDOUT_FILENO && i != STDERR_FILENO)
        close(i);
    }

    // 2. exec binlog instance
    std::string instance_bin_file = Config::instance().bin_path.val() + std::string("/") + "binlog_instance";
    char* argv[] = {const_cast<char*>("./binlog_instance"),
        const_cast<char*>(instance_work_path.c_str()),
        const_cast<char*>(INSTANCE_CONFIG_FILE),
        const_cast<char*>(tcp_port.c_str()),
        nullptr};
    execv(instance_bin_file.c_str(), argv);

    ::exit(-1);
  }
  obi_pid = pid;
  return OMS_OK;
}

int InstanceAsynTasks::correct_initial_gtid(
    uint64_t start_timestamp, std::vector<BinlogEntry*>& instance_entries, BinlogEntry& instance)
{
  std::string cluster = instance_entries.front()->cluster();
  std::string tenant = instance_entries.front()->tenant();
  OMS_INFO("Begin to select initial gtid mapping for instance [{}] with timestamp: {}",
      instance.instance_name(),
      start_timestamp);
  InstanceGtidSeq gtid_seq;
  if (OMS_OK != g_cluster->find_gtid_seq_by_timestamp(cluster, tenant, start_timestamp, gtid_seq)) {
    OMS_ERROR("Failed to find gtid seq within tenant [{}.{}] by timestamp: {}", cluster, tenant, start_timestamp);
    return OMS_FAILED;
  }
  if (gtid_seq.commit_version_start() > 0) {
    instance.update_initial_gtid(gtid_seq);
    OMS_INFO("Select the existing gtid seq [{}] of tenant [{}] used for initializing starting point for instance [{}]",
        gtid_seq.serialize_to_json(),
        instance.full_tenant(),
        instance.instance_name());
    return OMS_OK;
  }

  std::pair<InstanceGtidSeq, InstanceGtidSeq> boundary_gtid_pair;
  if (OMS_OK != g_cluster->boundary_gtid_seq(cluster, tenant, boundary_gtid_pair)) {
    OMS_ERROR("Failed to obtain boundary gtid seq of tenant: {}.{}", cluster, tenant);
    return OMS_FAILED;
  }
  OMS_INFO("Tenant [{}] boundary gtid seq: [{}, {}]",
      instance.full_tenant(),
      boundary_gtid_pair.first.serialize_to_json(),
      boundary_gtid_pair.second.serialize_to_json());

  if (start_timestamp < boundary_gtid_pair.first.commit_version_start()) {
    instance.update_initial_gtid(boundary_gtid_pair.first);
    OMS_INFO("Select minimal gtid seq [{}] of tenant [{}] used for initializing starting point for instance [{}]",
        boundary_gtid_pair.first.serialize_to_json(),
        instance.full_tenant(),
        instance.instance_name());
    return OMS_OK;
  }

  BinlogEntry selected_instance;
  SelectionStrategy::filter_instance_by_initial_point(start_timestamp, instance_entries, selected_instance);
  instance.update_initial_gtid(selected_instance);
  OMS_INFO(
      "Tenant [{}] has no gtid seq yet, and select the base instance [{}] used for initializing starting point for "
      "instance [{}]",
      instance.full_tenant(),
      selected_instance.instance_name(),
      instance.instance_name());
  return OMS_OK;
}

int InstanceAsynTasks::init_starting_point(std::vector<BinlogEntry*>& instance_entries, BinlogEntry& instance)
{
  std::map<uint8_t, uint8_t> state_distribution;
  for (auto iter = instance_entries.begin(); iter != instance_entries.end();) {
    state_distribution[(*iter)->state()] += 1;
    if ((*iter)->is_dropped() || (*iter)->state() == InstanceState::INIT) {
      delete (*iter);
      iter = instance_entries.erase(iter);
    } else {
      iter++;
    }
  }
  OMS_INFO("For creating instance [{}], the state distribution of instances(total: {}) for tenant: {}.{}, INIT: {}, "
           "STARTING: {}, RUNNING: {}, FAILED: {}, STOP: {}, OFFLINE: {}, GRAYSCALE: {}, DROP: {}, LOGICAL_DROP: {}",
      instance.instance_name(),
      instance_entries.size(),
      instance.cluster(),
      instance.tenant(),
      state_distribution[InstanceState::INIT],
      state_distribution[InstanceState::STARTING],
      state_distribution[InstanceState::RUNNING],
      state_distribution[InstanceState::FAILED],
      state_distribution[InstanceState::STOP],
      state_distribution[InstanceState::OFFLINE],
      state_distribution[InstanceState::GRAYSCALE],
      state_distribution[InstanceState::DROP],
      state_distribution[InstanceState::LOGICAL_DROP]);

  // 1. instance is the first of tenant, and retain original starting point
  uint64_t ori_start_timestamp = instance.config()->cdc_config()->start_timestamp();
  uint64_t ori_gtid_seq = instance.config()->binlog_config()->initial_ob_txn_gtid_seq();
  std::string ori_trx_id = instance.config()->binlog_config()->initial_ob_txn_id();
  if (instance_entries.empty()) {
    OMS_INFO("Binlog instance [{}] is the first valid instance of tenant [{}], initial start timestamp: {}, "
             "initial gtid seq: {}, initial txn id: {}",
        instance.instance_name(),
        instance.full_tenant(),
        ori_start_timestamp,
        ori_gtid_seq,
        ori_trx_id);
    return OMS_OK;
  }

  // 2. correct starting gtid point
  if (OMS_OK != correct_initial_gtid(instance.config()->cdc_config()->start_timestamp(), instance_entries, instance)) {
    OMS_ERROR("Failed to correct initial gtid point for newly instance [{}]", instance.instance_name());
    return OMS_FAILED;
  }
  OMS_INFO("Corrected the starting point of instance [{}], initial start timestamp: {} -> {}, "
           "initial gtid: {} -> {}, initial txn_id: {} -> {}",
      instance.instance_name(),
      ori_start_timestamp,
      instance.config()->cdc_config()->start_timestamp(),
      ori_gtid_seq,
      instance.config()->binlog_config()->initial_ob_txn_gtid_seq(),
      ori_trx_id,
      instance.config()->binlog_config()->initial_ob_txn_id());
  return OMS_OK;
}

int init_recovery_point(BinlogEntry& instance, std::string& offline_instance_name)
{
  ObAccess ob_access;
  uint64_t min_clog_timestamp = 0;
  if (OMS_OK != ob_access.query_min_clog_timestamp(instance.config()->get_obcdc_config(), min_clog_timestamp) ||
      0 == min_clog_timestamp) {
    OMS_ERROR("Failed to get minimum clog timestamp: {}", min_clog_timestamp);
    return OMS_FAILED;
  }
  BinlogEntry offline_instance;
  if (OMS_OK != g_cluster->query_instance_by_name(offline_instance_name, offline_instance)) {
    OMS_ERROR("Failed to query meta of the offline instance: {}", offline_instance_name);
    return OMS_FAILED;
  }
  std::pair<InstanceGtidSeq, InstanceGtidSeq> boundary_gtid_pair;
  if (OMS_OK != g_cluster->boundary_gtid_seq(offline_instance_name, boundary_gtid_pair)) {
    OMS_ERROR("Failed to find boundary gtid seq of offline instance [{}]", offline_instance_name);
    return OMS_FAILED;
  }
  OMS_INFO("Offline instance [{}] boundary gtid: {}, {}",
      offline_instance_name,
      boundary_gtid_pair.first.serialize_to_json(),
      boundary_gtid_pair.second.serialize_to_json());

  uint64_t recover_start_timestamp = 0;
  if (strcmp(s_config.recovery_point_strategy.val().c_str(), "fast") == 0) {
    uint64_t min_dump_checkpoint =
        offline_instance.min_dump_checkpoint() == 0 ? Timer::now_s() : offline_instance.min_dump_checkpoint();
    // fallback 10 minutes
    min_dump_checkpoint = min_dump_checkpoint - 600;
    recover_start_timestamp = (min_dump_checkpoint < min_clog_timestamp) ? min_clog_timestamp : min_dump_checkpoint;
    OMS_INFO("Recovery fast, min dump checkpoint: {}, min clog timestamp: {}, recover start timestamp: {}",
        min_dump_checkpoint,
        min_clog_timestamp,
        recover_start_timestamp);
  } else {
    uint64_t min_gtid_timestamp = boundary_gtid_pair.first.commit_version_start();
    min_gtid_timestamp = (min_gtid_timestamp == 0) ? Timer::now_s() : min_gtid_timestamp;
    recover_start_timestamp = (min_gtid_timestamp < min_clog_timestamp) ? min_clog_timestamp : min_gtid_timestamp;
    OMS_ERROR("Recover full, min gtid timestamp: {}, min clog timestamp: {}, recover start timestamp: {}",
        min_gtid_timestamp,
        min_clog_timestamp,
        recover_start_timestamp);  // todo rich
  }

  InstanceGtidSeq gtid_seq;
  g_cluster->find_gtid_seq_by_timestamp(offline_instance_name, recover_start_timestamp, gtid_seq);
  if (gtid_seq.commit_version_start() == 0) {
    OMS_ERROR("Failed to find gtid seq by timestamp [{}] of offline instance [{}]",
        recover_start_timestamp,
        offline_instance_name);

    if (boundary_gtid_pair.first.commit_version_start() != 0) {
      OMS_INFO("Use first gtid mapping as recovery point: {}", boundary_gtid_pair.first.serialize_to_json());
      gtid_seq.assign(&(boundary_gtid_pair.first));
    } else if (boundary_gtid_pair.second.commit_version_start() != 0) {
      OMS_INFO("Use last gtid mapping as recovery point: {}", boundary_gtid_pair.first.serialize_to_json());
      gtid_seq.assign(&(boundary_gtid_pair.second));
    } else {
      OMS_ERROR("Offline instance [{}] doesn't have gtid", offline_instance_name);
      return OMS_FAILED;
    }
  }
  instance.config()->cdc_config()->set_start_timestamp(gtid_seq.commit_version_start());
  instance.config()->binlog_config()->set_initial_ob_txn_id(gtid_seq.xid_start());
  uint64_t txn_gtid_seq = gtid_seq.xid_start().empty() ? gtid_seq.gtid_start() + 1 : gtid_seq.gtid_start();
  instance.config()->binlog_config()->set_initial_ob_txn_gtid_seq(txn_gtid_seq);

  OMS_INFO("Init recovery point, start timestamp: {}, gtid <-> ob_trx: {} <-> {}",
      instance.config()->cdc_config()->start_timestamp(),
      instance.config()->binlog_config()->initial_ob_txn_gtid_seq(),
      instance.config()->binlog_config()->initial_ob_txn_id());
  return OMS_OK;
}

int InstanceAsynTasks::serialize_configs(InstanceMeta* meta, const string& config_file)
{
  // serialize json to buffer
  rapidjson::StringBuffer buffer;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);

  writer.StartObject();
  Config l_config = Config::instance();
  std::map<std::string, std::string> instance_configs;
  if (OMS_OK !=
      g_cluster->query_instance_configs(
          instance_configs, meta->slot_config()->group(), meta->cluster(), meta->tenant(), meta->instance_name())) {
    OMS_ERROR("Failed to query instance [{}] configs from metadb.", meta->instance_name());
  }
  l_config.init(instance_configs);
  l_config.to_json(writer);

  // sys user for login OBI
  User sys_user;
  if (OMS_OK != g_cluster->query_sys_user(sys_user) || sys_user.username().empty()) {
    OMS_ERROR("Failed to query instance sys user from metadb");
    return OMS_FAILED;
  }
  meta->binlog_config()->set_instance_user(sys_user.username());
  meta->binlog_config()->set_instance_password(sys_user.password());
  meta->binlog_config()->set_instance_password_sha1(sys_user.password_sha1());

  meta->update_version();
  writer.Key(INSTANCE_META);
  rapidjson::Document document;
  rapidjson::Value json_value = meta->serialize_to_json_value(document);
  json_value.Accept(writer);
  writer.EndObject();

  return FsUtil::write_file(config_file, buffer.GetString());
}

bool if_obi_bind_failed(std::string& instance_pid_file, int instance_pid, ObiInternalState& speculated_obi_state)
{
  int retry_count = 0;
  int is_exited = false;
  uint32_t max_retry_times = s_config.max_instance_startup_wait_sec.val() * 1000 / 100;
  while (++retry_count < max_retry_times) {
    std::string pid_content;
    if (FsUtil::exist(instance_pid_file)) {
      FsUtil::read_file(instance_pid_file, pid_content, true);
    }

    if (!pid_content.empty()) {
      int pid = std::atoi(pid_content.c_str());
      if (pid > 0) {
        speculated_obi_state = is_exited ? ObiInternalState::EXIT : ObiInternalState::OK;
      } else {
        speculated_obi_state = (ERR_ADDR_INUSE == pid) ? ObiInternalState::BIND_ERROR : ObiInternalState::EXIT;
      }
      return ObiInternalState::BIND_ERROR == speculated_obi_state;
    } else {
      if (is_exited) {
        speculated_obi_state = ObiInternalState::EXIT;
        return false;
      }
      is_exited = (0 != kill(instance_pid, 0));

      if (!is_exited) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
  }

  speculated_obi_state = ObiInternalState::MAY_HANG_ERROR;
  return false;
}

void InstanceAsynTasks::mark_instance_failover(bool recover, Task task)
{
  if (recover) {
    auto* task_param = (RecoverInstanceTaskParam*)task.task_param();
    PrometheusExposer::mark_binlog_counter_metric(task.execution_node(),
        task_param->offline_instance(),
        task_param->cluster(),
        task_param->tenant(),
        task_param->cluster_id(),
        task_param->tenant_id(),
        BINLOG_INSTANCE_FAILOVER_FAIL_COUNT_TYPE);
  }
}

int InstanceAsynTasks::create_binlog_instance(const std::string& cluster, const std::string& tenant,
    const std::string& instance_name, const std::string& task_id, bool recover)
{
  // 1. fetch instances of tenant
  std::vector<BinlogEntry*> instance_entries;
  defer(release_vector(instance_entries));
  BinlogEntry condition;
  condition.set_cluster(cluster);
  condition.set_tenant(tenant);
  g_cluster->query_instances(instance_entries, condition);

  // get BinlogEntry of instance and remove itself from instance_entries
  BinlogEntry instance;
  for (auto iter = instance_entries.begin(); iter != instance_entries.end();) {
    if (strcmp((*iter)->instance_name().c_str(), instance_name.c_str()) == 0) {
      instance.assign((*iter));
      if (nullptr != *iter) {
        delete *iter;
        *iter = nullptr;
      }
      instance_entries.erase(iter);
      break;
    } else {
      iter++;
    }
  }

  // 2. get current task
  Task task;
  if (OMS_OK != g_cluster->query_task_by_id(task_id, task)) {
    OMS_ERROR("Failed to query task: {}", task_id);
    return OMS_FAILED;
  }

  defer(g_cluster->update_task(task));
  defer(g_cluster->update_instance(instance));
  if (TaskStatus::EXECUTING != task.status() && InstanceState::STARTING != instance.state()) {
    OMS_ERROR("When [{}] binlog instance [{}], the task status expected is [Executing] rather than [{}], and the "
              "instance status expected is [Staring] rather than [{}]",
        TaskType::CREATE_INSTANCE == task.type() ? "create" : "recover",
        instance.instance_name(),
        task_status_str(task.status()),
        instance_state_str(instance.state()));
    instance.set_state(InstanceState::FAILED);
    task.set_status(TaskStatus::FAILED);  // todo  think more.. keep original status or set status to failed
    return OMS_FAILED;
  }
  instance.set_state(InstanceState::FAILED);
  task.set_status(TaskStatus::FAILED);

  // 3. init the starting gtid <-> ob_trx mapping
  bool in_place = true;
  int ret = OMS_OK;
  if (recover) {
    auto* task_param = (RecoverInstanceTaskParam*)task.task_param();
    std::string offline_instance_name = task_param->offline_instance();
    in_place = (strcmp(instance.instance_name().c_str(), offline_instance_name.c_str()) == 0);
    // when restarting in place, do nothing here and OBI will initialize point based on the breakpoint binlog
    if (!in_place) {
      ret = init_recovery_point(instance, offline_instance_name);
    }
  } else {
    ret = init_starting_point(instance_entries, instance);
  }
  if (OMS_OK != ret) {
    OMS_ERROR("Failed to init starting/recover checkpoint for instance [{}] when {}",
        instance_name,
        recover ? "recovering" : "creating");
    mark_instance_failover(recover, task);
    return OMS_FAILED;
  }

  // 4. check whether the work path is occupied or not
  std::string instance_work_path = s_config.binlog_log_bin_basename.val() + "/" + instance_name;
  bool is_existed = true;
  if (is_work_path_occupied(instance_work_path, is_existed)) {
    OMS_ERROR(
        "The work path [{}] of the [{}] instance [{}] is occupied by other process, and the instance may be alive.",
        instance_work_path,
        recover ? "recovering" : "creating",
        instance.instance_name());
    if (recover && in_place) {
      OMS_INFO("In the scenario of recovering in place, the work path of instance [{}] has been occupied may be due to "
               "a deviation in survivability detection, so the recovery action is no longer performed.",
          instance.instance_name());
      instance.set_state(InstanceState::RUNNING);
      task.set_status(TaskStatus::COMPLETED);
      try_elect_master(instance);
      return OMS_OK;
    }

    mark_instance_failover(recover, task);
    return OMS_FAILED;
  }
  if (is_existed && (!recover || (!in_place))) {
    if (s_config.binlog_recover_backup.val()) {
      std::string dst_dir = s_config.binlog_log_bin_basename.val() + "/recover_backup/" + instance_name + "_backup_" +
                            std::to_string(Timer::now_s());
      FsUtil::backup_dir(instance_work_path, dst_dir);
    }

    FsUtil::remove(instance_work_path);
    OMS_WARN("{} the working directory [{}] left by binlog instance: {}",
        s_config.binlog_recover_backup.val() ? "Backup and remove" : "Remove",
        instance_work_path,
        instance.instance_name());
  }
  if (!FsUtil::mkdir(instance_work_path)) {
    OMS_ERROR("Failed to create work path [{}] for instance [{}]", instance_work_path, instance_name);
    return OMS_FAILED;
  }
  instance.set_work_path(instance_work_path);

  // old OBI missing server_id/server_uuid configuration
  check_or_config_server_option(instance);
  // 5. serialize instance meta to file
  std::string config_file = instance_work_path + string("/") + std::string(INSTANCE_CONFIG_FILE);
  if (OMS_OK != serialize_configs(instance.config(), config_file)) {
    OMS_ERROR("Failed to serialize configs of binlog instance process to file: {}", config_file);
    mark_instance_failover(recover, task);
    return OMS_FAILED;
  }

  OMS_INFO("Begin to create OceanBase binlog instance [{}](recover: {}, in_place: {}) for [{}.{}] with start "
           "timestamp(s): {}, config: {}",
      instance_name,
      recover,
      in_place,
      cluster,
      tenant,
      instance.config()->cdc_config()->start_timestamp(),
      instance.config()->serialize_to_json());

  // 6. start binlog instance
  ret = start_binlog_instance_inner(instance);
  if (OMS_OK == ret) {
    task.set_status(TaskStatus::COMPLETED);
    try_elect_master(instance);
  } else {
    mark_instance_failover(recover, task);
  }
  if (!in_place) {
    std::string offline_instance = ((RecoverInstanceTaskParam*)task.task_param())->offline_instance();
    drop_remote_failover_instance_logically(offline_instance);
  }
  return ret;
}

int InstanceAsynTasks::start_binlog_instance(
    const string& instance_name, const hsql::InstanceFlag& flag, const std::string& task_id)
{
  // get current task
  Task task;
  g_cluster->query_task_by_id(task_id, task);
  task.set_status(TaskStatus::FAILED);
  defer(g_cluster->update_task(task));

  BinlogEntry instance;
  if (OMS_OK != g_cluster->query_instance_by_name(instance_name, instance)) {
    OMS_ERROR("Failed to query meta for instance: {}", instance_name);
    return OMS_FAILED;
  }

  instance.set_state(InstanceState::FAILED);
  defer(g_cluster->update_instance(instance));

  // check work path
  bool is_existed = true;
  int ret = OMS_OK;
  if (is_work_path_occupied(instance.work_path(), is_existed)) {
    OMS_ERROR("The work path [{}] of instance [{}] is occupied by other process, and the instance may be alive",
        instance.work_path(),
        instance.instance_name());
    instance.set_state(InstanceState::RUNNING);
  } else {
    // old OBI missing server_id/server_uuid configuration
    check_or_config_server_option(instance);
    // re-serialize instance meta to file
    std::string config_file = instance.work_path() + string("/") + std::string(INSTANCE_CONFIG_FILE);
    if (OMS_OK != serialize_configs(instance.config(), config_file)) {
      OMS_ERROR("Failed to serialize configs of binlog instance process to file: {}", config_file);
      return OMS_FAILED;
    }

    // start binlog instance via fork
    OMS_INFO("Begin to start binlog instance [{}] with meta: {}", instance_name, instance.serialize_to_json());
    ret = start_binlog_instance_inner(instance);
  }

  if (OMS_OK == ret) {
    task.set_status(TaskStatus::COMPLETED);
    try_elect_master(instance);
  }
  return ret;
}

int InstanceAsynTasks::start_binlog_instance_inner(BinlogEntry& instance)
{
  int instance_pid;
  uint16_t tcp_port = 0;
  int retry_times = 0;
  std::string instance_work_path = instance.work_path();
  ObiInternalState speculated_obi_state = ObiInternalState::EXIT;
  std::string instance_pid_file = get_instance_pid_file(instance_work_path);
  do {
    if (0 != tcp_port) {
      OMS_INFO("Bind error with port: {}, and try to start binlog instance again with retry times: [{}]",
          tcp_port,
          ++retry_times);
    }

    // 1. take a tcp port
    if (OMS_OK != binlog::g_tcp_port_pool->take_port(tcp_port)) {
      OMS_ERROR("No more available tcp port.");
      break;
    }

    // 2. create binlog instance process via fork
    if (OMS_OK != invoke_fork(instance_work_path, std::to_string(tcp_port), instance_pid)) {
      break;
    }
  } while (if_obi_bind_failed(instance_pid_file, instance_pid, speculated_obi_state));

  // update binlog instance state
  if (ObiInternalState::OK != speculated_obi_state) {
    OMS_ERROR("Failed to start binlog instance [{}] process for tenant [{}.{}] with speculated state: {}",
        instance.instance_name(),
        instance.cluster(),
        instance.tenant(),
        to_string(speculated_obi_state));
    instance.set_state(InstanceState::FAILED);
    return OMS_FAILED;
  }

  OMS_INFO("+++ Start binlog instance [{}] for [{}.{}] with pid: {}, listening port: {}",
      instance.instance_name(),
      instance.cluster(),
      instance.tenant(),
      instance_pid,
      tcp_port);
  instance.set_port(tcp_port);
  instance.set_pid(instance_pid);
  instance.set_state(InstanceState::RUNNING);
  return OMS_OK;
}

int InstanceAsynTasks::stop_binlog_instance(
    const string& instance_name, const hsql::InstanceFlag& flag, const std::string& task_id)
{
  // get current task
  Task task;
  g_cluster->query_task_by_id(task_id, task);
  task.set_status(TaskStatus::FAILED);
  defer(g_cluster->update_task(task));

  // get current binlog instance
  BinlogEntry instance;
  if (OMS_OK != g_cluster->query_instance_by_name(instance_name, instance)) {
    OMS_ERROR("Failed to query meta for instance: {}", instance_name);
    return OMS_FAILED;
  }

  kill_binlog_instance(instance);
  OMS_INFO("Successfully stop binlog instance: {}", instance.instance_name());
  PrometheusExposer::mark_binlog_counter_metric(instance.node_id(),
      instance.instance_name(),
      instance.cluster(),
      instance.tenant(),
      instance.cluster_id(),
      instance.tenant_id(),
      BINLOG_INSTANCE_STOP_TYPE);
  instance.set_state(InstanceState::STOP);
  if (OMS_OK != g_cluster->update_instance(instance)) {
    OMS_ERROR("{}: Failed to update instance {} state from [{}] to [STOP]",
        task_id,
        instance.instance_name(),
        instance_state_str(instance.state()));
    return OMS_FAILED;
  }
  task.set_status(TaskStatus::COMPLETED);

  if (is_master_instance(instance)) {
    master_election(instance.cluster(), instance.tenant(), instance.instance_name(), instance.min_dump_checkpoint());
  }
  return OMS_OK;
}

int InstanceAsynTasks::drop_binlog_instance(const std::string& instance_name, const std::string& task_id, bool drop_all)
{
  // get current task
  Task task;
  g_cluster->query_task_by_id(task_id, task);
  task.set_status(TaskStatus::FAILED);
  defer(g_cluster->update_task(task));

  // get current binlog instance
  BinlogEntry instance;
  if (OMS_OK != g_cluster->query_instance_by_name(instance_name, instance)) {
    OMS_ERROR("Failed to query meta for instance: {}", instance_name);
    return OMS_FAILED;
  }

  OMS_INFO("Begin to drop binlog instance process: {}(status: {})",
      instance.to_string(),
      instance_state_str(instance.state()));
  kill_binlog_instance(instance);
  OMS_INFO("Successfully kill binlog instance process: {}, and begin to clean work path: {}",
      instance.instance_name(),
      instance.work_path());

  // Clean up related binlog files and working directories
  std::string work_path = instance.work_path();
  if (!work_path.empty()) {
    if (work_path.find(instance.instance_name()) != string::npos) {
      error_code err;
      uintmax_t num = std::filesystem::remove_all(work_path, err);
      if (err) {
        OMS_ERROR("Failed to release instance and clean file, error: {}", err.message());
      }
      OMS_INFO("Deleted {} files or directories", num);
    } else {
      OMS_ERROR("There may be a problem with the currently cleaned log directory: {}", work_path);
    }
  }

  OMS_INFO("Successfully drop instance cluster: {},tenant: {}, instance name: {}, pid: {}",
      instance.cluster(),
      instance.tenant(),
      instance.instance_name(),
      instance.pid());
  PrometheusExposer::mark_binlog_counter_metric(instance.node_id(),
      instance.instance_name(),
      instance.cluster(),
      instance.tenant(),
      instance.cluster_id(),
      instance.tenant_id(),
      BINLOG_INSTANCE_RELEASE_TYPE);
  instance.set_state(InstanceState::DROP);
  if (OMS_OK != g_cluster->update_instance(instance)) {
    OMS_ERROR("{}: Failed to update instance {} state from [{}] to [DROP]",
        task_id,
        instance.instance_name(),
        instance_state_str(instance.state()));
    return OMS_FAILED;
  }
  task.set_status(TaskStatus::COMPLETED);

  if (OMS_OK != g_cluster->remove_instance_gtid_seq(instance_name)) {
    OMS_ERROR("Failed to remove gtid seqs of the dropped instance: {}", instance_name);
  }

  g_cluster->delete_config("", Granularity::G_INSTANCE, instance_name);

  if (is_master_instance(instance)) {
    master_election(
        instance.cluster(), instance.tenant(), instance.instance_name(), instance.min_dump_checkpoint(), drop_all);
  }
  return OMS_OK;
}

void InstanceAsynTasks::kill_binlog_instance(const BinlogEntry& instance)
{
  int pid = (int)instance.pid();
  if (pid <= 0) {
    OMS_WARN("Invalid pid [{}] for binlog instance: {}.", pid, instance.instance_name());
    return;
  }

  OMS_INFO("Begin to kill binlog instance process: {}, pid: {}", instance.instance_name(), pid);
  if (instance.state() == InstanceState::RUNNING || instance.state() == InstanceState::GRAYSCALE ||
      instance.state() == InstanceState::OFFLINE || instance.state() == InstanceState::LOGICAL_DROP) {
    int ret = kill(pid, SIGKILL);
    if (ret != 0) {
      OMS_ERROR("Failed to kill binlog instance process: {}, error: {}",
          instance.instance_name(),
          logproxy::system_err(errno));
    }
  } else {
    OMS_INFO("Binlog instance [{}] process(status: {}) is not alive, and no need to do kill",
        instance.instance_name(),
        instance.state());
  }
  g_tcp_port_pool->return_port(instance.port());
}

bool InstanceAsynTasks::is_work_path_occupied(const std::string& work_path, bool& is_existed)
{
  std::string instance_pid_file = get_instance_pid_file(work_path);
  if (!FsUtil::exist(instance_pid_file)) {
    is_existed = false;
    return false;
  }
  FILE* file = FsUtil::fopen_binary(instance_pid_file);
  defer(FsUtil::fclose_binary(file));
  if (nullptr == file) {
    return false;
  }
  int fd = fileno(file);
  return _check_flock(fd);
}

void InstanceAsynTasks::try_elect_master(BinlogEntry& instance)
{
  OMS_INFO("Newly instance [{}] try to elect master instance of tenant [{}]",
      instance.instance_name(),
      instance.full_tenant());

  std::string curr_master_instance_name;
  if (OMS_OK != g_cluster->query_master_instance(instance.cluster(), instance.tenant(), curr_master_instance_name)) {
    OMS_ERROR("Failed to query master instance for tenant [{}]", instance.full_tenant());
    return;
  }
  if (!curr_master_instance_name.empty()) {
    OMS_INFO("Master instance [{}] of tenant [{}] is existed.", curr_master_instance_name, instance.full_tenant());
    BinlogEntry curr_master_instance;
    if (OMS_OK != g_cluster->query_instance_by_name(curr_master_instance_name, curr_master_instance)) {
      return;
    }
    if (!curr_master_instance.instance_name().empty() && !curr_master_instance.is_dropped()) {
      OMS_INFO("Master instances actually exist and is not in drop status.");
      return;
    }

    OMS_ERROR(
        "Master instance [{}] does not exist or is in drop status, and there may be dirty data; therefore, reset the "
        "master instance of tenant: {}",
        curr_master_instance_name,
        instance.full_tenant());
    if (OMS_OK != g_cluster->reset_master_instance(instance.cluster(), instance.tenant(), curr_master_instance_name)) {
      OMS_ERROR(
          "Failed to reset master instance [{}] of tenant: {}", curr_master_instance_name, instance.full_tenant());
      return;
    }
  }

  std::string new_master_instance;
  if (OMS_OK != g_cluster->determine_primary_instance(
                    instance.cluster(), instance.tenant(), instance.instance_name(), new_master_instance)) {
    OMS_ERROR("Failed to elect master instance for tenant [{}]", instance.full_tenant());
    return;
  }

  OMS_INFO("[{}]: Elected master instance [{}], and master binlog instance actually: {}",
      instance.full_tenant(),
      instance.instance_name(),
      new_master_instance);
}

void InstanceAsynTasks::check_or_config_server_option(BinlogEntry& instance)
{
  if (instance.config()->binlog_config()->server_id() == 0 ||
      instance.config()->binlog_config()->server_uuid().empty()) {
    std::vector<BinlogEntry> instance_vec;
    std::string cluster = instance.cluster();
    std::string tenant = instance.tenant();
    g_cluster->query_tenant_surviving_instances(cluster, tenant, instance_vec);
    BinlogInstanceHelper::check_or_config_instance_server_options(instance_vec, instance.ip(), instance.config());
    OMS_INFO("Instance [{}] lacks its own server_id/server_uuid, and reconfigure server_id [{}], server_uuid [{}]",
        instance.instance_name(),
        instance.config()->binlog_config()->server_id(),
        instance.config()->binlog_config()->server_uuid());
  }
}

void InstanceAsynTasks::drop_remote_failover_instance_logically(string& offline_instance)
{
  BinlogEntry instance(true);
  g_cluster->query_instance_by_name(offline_instance, instance);
  if (instance.instance_name().empty()) {
    OMS_ERROR("Failed to query offline instance: {}", offline_instance);
    return;
  }

  instance.set_state(InstanceState::LOGICAL_DROP);  // todo publish task?
  if (OMS_OK != g_cluster->update_instance(instance)) {
    OMS_ERROR("Failed to update offline instance state from {} to LogicalDrop", instance_state_str(instance.state()));
  }
}

void TaskCallback::task_executor(Task& task)
{
  switch (task.status()) {
    case TaskStatus::WAITING:
    case TaskStatus::FAILED:
      handle_waiting_or_failed_task(task);
      break;
    case TaskStatus::EXECUTING:
      handle_executing_task(task);
      break;
    default:
      OMS_ERROR("Unsupported task status: {}", task_status_str(task.status()));
  }
}

void TaskCallback::handle_waiting_or_failed_task(Task& task)
{
  uint64_t retry_count = task.retry_count();
  std::string instance_name = task.instance_name();
  BinlogEntry instance;
  if (OMS_OK != binlog::g_cluster->query_instance_by_name(instance_name, instance)) {
    OMS_ERROR("Failed to query instance [{}] related to task [{}]", instance_name, task.task_id());
    task.set_status(TaskStatus::FAILED);
    binlog::g_cluster->update_task(task);
    return;
  }
  if (instance.instance_name().empty()) {
    OMS_ERROR("The instance [{}] which task [{}] belong to no longer exists.", instance_name, task.task_id());
    task.set_status(TaskStatus::FAILED);
    binlog::g_cluster->update_task(task);
    return;
  }
  if (task.type() != TaskType::RECOVER_INSTANCE &&
      strcmp(instance.node_id().c_str(), g_cluster->get_node_info().id().c_str()) != 0) {
    OMS_ERROR("May be dirty data, since the node [{}] of instance [{}] with status [{}] does not match the executed "
              "node: {}",
        instance.node_id(),
        instance.instance_name(),
        instance_state_str(instance.state()),
        g_cluster->get_node_info().id());
    task.set_status(TaskStatus::COMPLETED);
    binlog::g_cluster->update_task(task);
    return;
  }

  auto task_handler_iter = _s_task_handlers.find(static_cast<TaskType>(task.type()));
  if (task_handler_iter == _s_task_handlers.end()) {
    OMS_ERROR("Unsupported task type: {}", task.type());
    task.set_status(TaskStatus::FAILED);
    binlog::g_cluster->update_task(task);
    return;
  }

  if (task_handler_iter->second->execNow(task)) {
    if (TaskStatus::FAILED == task.status()) {
      if (retry_count >= MAX_TASK_RETRY_TIMES) {
        return;
      }
      retry_count += 1;
      if (retry_count == MAX_TASK_RETRY_TIMES - 1) {
        OMS_ERROR(
            "Reached max retry times to execute task [{}] for handle instance [{}], and do the last retry [{}] ...",
            MAX_TASK_RETRY_TIMES,
            task.task_id(),
            instance_name,
            retry_count);
      } else {
        OMS_WARN("Failed to execute task [{}] for handling instance [{}], and do retry [{}] ...",
            task.task_id(),
            instance_name,
            retry_count);
      }
    }

    // 1. update task state: WAITING -> EXECUTING
    task.set_status(TaskStatus::EXECUTING);
    task.set_retry_count(retry_count);
    binlog::g_cluster->update_task(task);

    // 2. handle task
    task_handler_iter->second->execute(task, instance);
  }
}

void TaskCallback::handle_executing_task(Task& task)
{
  if (Timer::now_s() - task.last_modify() > s_config.max_task_execution_time_s.val()) {
    std::string instance_name = task.instance_name();
    OMS_INFO("The [{}] task [{}] for binlog instance [{}] has been executing for {} seconds, and may have been "
             "interrupted",
        task_type_str(task.type()),
        task.task_id(),
        instance_name,
        s_config.max_task_execution_time_s.val());

    defer(g_cluster->update_task(task));
    BinlogEntry instance;
    if (OMS_OK != binlog::g_cluster->query_instance_by_name(instance_name, instance)) {
      OMS_ERROR("Failed to query instance {} related to task {}", instance_name, task.task_id());
      task.set_status(TaskStatus::FAILED);
      return;
    }
    if (instance.instance_name().empty()) {
      OMS_ERROR("The binlog instance [{}] corresponding to executing task [{}] no longer exists",
          instance_name,
          task.task_id());
      task.set_status(TaskStatus::FAILED);
      return;
    }

    defer(g_cluster->update_instance(instance));
    // handle task
    auto task_handler_iter = _s_task_handlers.find(static_cast<TaskType>(task.type()));
    if (task_handler_iter == _s_task_handlers.end()) {
      OMS_ERROR("Unsupported task type: {}", task.type());
      return;
    }

    task_handler_iter->second->interrupt_recover(task, instance);
    OMS_INFO("Recover interrupted task status: {}, instance status: {}", task.status(), instance.state());
  }
}

void InstanceFailoverCallback::instance_failover(const BinlogEntry& offline_instance)
{
  OMS_INFO("Begin to failover the offline instance [{}] for tenant {}",
      offline_instance.instance_name(),
      offline_instance.full_tenant());

  if (is_master_instance(offline_instance)) {
    OMS_INFO("The master instance [{}] has been offline, and begin to elect master instance.",
        offline_instance.instance_name());
    master_election(offline_instance.cluster(),
        offline_instance.tenant(),
        offline_instance.instance_name(),
        offline_instance.min_dump_checkpoint());
  }

  recover_offline_instance(offline_instance);
}

bool is_master_instance(const BinlogEntry& offline_instance)
{
  std::string master_instance;
  if (OMS_OK !=
      g_cluster->query_master_instance(offline_instance.cluster(), offline_instance.tenant(), master_instance)) {
    OMS_ERROR(
        "Failed to query master instance for tenant {}.{}", offline_instance.cluster(), offline_instance.tenant());
  }
  return (strcmp(master_instance.c_str(), offline_instance.instance_name().c_str()) == 0);
}

void master_election(const std::string& cluster, const std::string& tenant, const std::string& prev_master_instance,
    uint64_t min_dump_checkpoint, bool only_reset)
{
  OMS_INFO("Previous master instance [{}] with min dump checkpoint [{}] is unavailable and reset master "
           "instance only: {}",
      prev_master_instance,
      min_dump_checkpoint,
      only_reset);
  g_cluster->reset_master_instance(cluster, tenant, prev_master_instance);
  if (only_reset) {
    return;
  }

  OMS_INFO("Begin to select master instance of tenant [{}.{}]", cluster, tenant);
  BinlogEntry condition;
  condition.set_cluster(cluster);
  condition.set_tenant(tenant);

  BinlogEntry election_instance;
  Timer timer;
  do {
    std::vector<BinlogEntry*> instance_entries;
    defer(release_vector(instance_entries));
    g_cluster->query_instances(instance_entries, condition);
    std::map<int, std::vector<BinlogEntry>> state_instances_map;
    for (auto entry : instance_entries) {
      state_instances_map[entry->state()].emplace_back((*entry));
    }

    if (instance_entries.empty() ||
        (state_instances_map[InstanceState::DROP].size() + state_instances_map[InstanceState::LOGICAL_DROP].size()) ==
            instance_entries.size()) {
      OMS_ERROR("Tenant [{}.{}] has no instances, so master election failed", cluster, tenant);
      break;
    } else if ((state_instances_map[InstanceState::DROP].size() +
                   state_instances_map[InstanceState::LOGICAL_DROP].size() +
                   state_instances_map[InstanceState::STOP].size() + state_instances_map[InstanceState::FAILED].size() +
                   state_instances_map[InstanceState::OFFLINE].size()) == instance_entries.size()) {
      OMS_ERROR("Tenant [{}.{}] has no instances in running status, so master election failed", cluster, tenant);
      break;
    } else if (!state_instances_map[InstanceState::RUNNING].empty()) {
      std::vector<BinlogEntry> running_instances = state_instances_map[InstanceState::RUNNING];
      std::string err_msg;
      if (OMS_OK != SelectionStrategy::select_master_instance(
                        running_instances, min_dump_checkpoint, election_instance, err_msg)) {
        OMS_ERROR("Failed to select master instance, error: {}", err_msg);
      }
      break;
    } else if (!state_instances_map[InstanceState::INIT].empty() ||
               !state_instances_map[InstanceState::STARTING].empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    } else {
      OMS_ERROR("Unexpected instance state(num: {}) of tenant [{}.{}]", state_instances_map.size(), cluster, tenant);
    }
  } while (timer.elapsed() < (15000000));

  if (election_instance.instance_name().empty()) {
    OMS_ERROR("No available master instance of tenant [{}.{}]", cluster, tenant);
    PrometheusExposer::mark_binlog_counter_metric(election_instance.node_id(),
        prev_master_instance,
        cluster,
        tenant,
        election_instance.cluster_id(),
        election_instance.tenant_id(),
        BINLOG_INSTANCE_NO_MASTER_COUNT_TYPE);
    return;
  }

  if (!election_instance.instance_name().empty() && 0 != election_instance.port()) {
    std::string master_instance_actually;
    if (OMS_OK != g_cluster->determine_primary_instance(
                      cluster, tenant, election_instance.instance_name(), master_instance_actually)) {
      OMS_ERROR("[{}]: Failed to elect master instance [{}], and current master instance: {}",
          election_instance.full_tenant(),
          election_instance.instance_name(),
          master_instance_actually);
      return;
    }

    OMS_INFO("[{}]: Elected master instance [{}], and master binlog instance in actual: {}",
        election_instance.full_tenant(),
        election_instance.instance_name(),
        master_instance_actually);
  }
}

void InstanceFailoverCallback::recover_offline_instance(const BinlogEntry& offline_instance)
{
  if (0 == offline_instance.config()->binlog_config()->failover()) {
    OMS_INFO(
        "The [failover] config of instance [{}] is [false], and stop to recover", offline_instance.instance_name());
    return;
  }
  Task recent_offline_task;
  g_cluster->fetch_recent_offline_task(offline_instance.instance_name(), recent_offline_task);
  if (!recent_offline_task.task_id().empty()) {
    if (Timer::now_s() - recent_offline_task.last_modify() < 300) {
      OMS_ERROR("Recovered offline instance [{}] has failed three times in a row within 300 seconds; stop automatic "
                "recover and require manual intervention.",
          offline_instance.instance_name());
      PrometheusExposer::mark_binlog_counter_metric(recent_offline_task.execution_node(),
          offline_instance.instance_name(),
          offline_instance.cluster(),
          offline_instance.tenant(),
          offline_instance.cluster_id(),
          offline_instance.tenant_id(),
          BINLOG_INSTANCE_FAILOVER_FAIL_COUNT_TYPE);
      return;
    }
  }

  Node node;
  if (OMS_OK != g_cluster->query_node_by_id(offline_instance.node_id(), node)) {
    OMS_ERROR("Failed to query node [{}] where instance [{}] is located.",
        offline_instance.node_id(),
        offline_instance.instance_name());
    return;
  }

  std::string cluster = offline_instance.cluster();
  std::string tenant = offline_instance.tenant();
  OMS_INFO(
      "Begin to recover the offline instance [{}] for tenant {}.{}", offline_instance.instance_name(), cluster, tenant);

  Task recover_task(true);
  recover_task.set_type(TaskType::RECOVER_INSTANCE);
  recover_task.set_status(TaskStatus::WAITING);
  auto* param = new RecoverInstanceTaskParam();
  param->set_cluster(cluster);
  param->set_tenant(tenant);
  param->set_offline_instance(offline_instance.instance_name());
  recover_task.set_task_param(param);

  if (State::ONLINE == node.state()) {
    recover_task.init_recover_task(node, *offline_instance.config(), offline_instance.instance_name());
    g_cluster->publish_task(recover_task);
    OMS_INFO("The node [{}] where the offline instance [{}] is located is online, and do recover in place.",
        offline_instance.node_id(),
        offline_instance.instance_name());
  } else {
    // 1. check instance num
    std::vector<BinlogEntry> instance_vec;
    if (OMS_OK != g_cluster->query_tenant_surviving_instances(cluster, tenant, instance_vec)) {
      OMS_ERROR("Failed to obtain instances for tenant [{}.{}]", cluster, tenant);
      return;
    }
    if (instance_vec.size() > s_config.max_instance_replicate_num.val()) {
      OMS_ERROR(
          "The number of existing instances has reached limit: {} > {}, and remote-recovery will no longer continue",
          instance_vec.size(),
          s_config.max_instance_replicate_num.val());
      return;
    }

    // 2. remote recovery
    auto* instance_meta = dynamic_cast<InstanceMeta*>(offline_instance.config()->clone());
    if (!instance_meta->slot_config()->ip().empty()) {
      OMS_INFO("Removed the IP({}) option specified when creating the offline instance: {}",
          instance_meta->slot_config()->ip(),
          offline_instance.instance_name());
      instance_meta->slot_config()->set_ip("");
    }
    BinlogInstanceHelper::config_instance_name(instance_vec, instance_meta);

    std::string err_msg;
    Node new_node;
    if (OMS_OK != SelectionStrategy::load_balancing_node(*instance_meta, new_node, err_msg)) {
      OMS_ERROR("Failed to allocate node for recovering offline instance: {}", instance_meta->instance_name());
      PrometheusExposer::mark_binlog_counter_metric(recent_offline_task.execution_node(),
          offline_instance.instance_name(),
          offline_instance.cluster(),
          offline_instance.tenant(),
          offline_instance.cluster_id(),
          offline_instance.tenant_id(),
          BINLOG_INSTANCE_FAILOVER_FAIL_COUNT_TYPE);
      return;
    }
    BinlogInstanceHelper::config_instance_server_options(instance_vec, new_node.ip(), instance_meta);
    OMS_INFO("Begin to remotely recover instance [{}] with meta: {}",
        instance_meta->instance_name(),
        instance_meta->serialize_to_json());

    BinlogEntry instance(true);
    instance.init_instance(new_node, instance_meta);
    recover_task.init_recover_task(new_node, *instance_meta, offline_instance.instance_name());

    g_cluster->add_instance(instance, recover_task);
    OMS_INFO("The node [{}] where the offline instance [{}] is located is offline, and instance is loaded onto node "
             "[{}] with newly name [{}] for recovery",
        node.identifier(),
        offline_instance.instance_name(),
        recover_task.execution_node(),
        instance_meta->instance_name());
  }
}

void CreateTaskHandler::execute(Task& task, BinlogEntry& instance)
{
  if ((TaskStatus::WAITING == task.status() && instance.state() != InstanceState::INIT) ||
      (TaskStatus::FAILED == task.status() && instance.state() != InstanceState::FAILED)) {
    OMS_ERROR("May be dirty data, since the instance [{}] status expected by [create instance] task [{}] in "
              "[{}] status is [{}] rather than [{}]",
        instance.instance_name(),
        task.task_id(),
        task_status_str(task.status()),
        TaskStatus::WAITING == task.status() ? "Init" : "Failed",
        instance_state_str(instance.state()));
    return;
  }
  instance.set_state(binlog::InstanceState::STARTING);
  if (OMS_OK != binlog::g_cluster->update_instance(instance)) {
    OMS_ERROR("Failed to update binlog instance [{}] state to [starting]", instance.instance_name());
    return;
  }
  auto* task_param = (CreateInstanceTaskParam*)task.task_param();
  OMS_INFO("Submit a task(id: {}) to create binlog instance: {}", task.task_id(), instance.instance_name());
  g_executor->submit(binlog::InstanceAsynTasks::create_binlog_instance,
      task_param->cluster(),
      task_param->tenant(),
      instance.instance_name(),
      task.task_id(),
      false);
}

void CreateTaskHandler::interrupt_recover(Task& task, BinlogEntry& instance)
{
  OMS_INFO("Begin to recover [create instance] task for instance: {}", instance.serialize_to_json());
  switch (instance.state()) {
    case InstanceState::INIT:
      task.set_status(TaskStatus::WAITING);
      break;
    case InstanceState::RUNNING:
    case InstanceState::GRAYSCALE:
      task.set_status(TaskStatus::COMPLETED);
      break;
    case InstanceState::FAILED:
      task.set_status(TaskStatus::FAILED);
      break;
    case InstanceState::STARTING: {
      recover_starting_instance(task, instance);
      break;
    }
    default:
      OMS_ERROR("Unsupported instance status with create task in executing status: {}", instance.state());
  }
}

bool DropTaskHandler::execNow(Task& task)
{
  auto* task_param = (DropInstanceTaskParam*)task.task_param();
  uint64_t expect_exec_time = task_param->expect_exec_time();
  int64_t interval = expect_exec_time - Timer::now_s();

  if (interval > 0) {
    if (interval % 60 == 0) {
      OMS_INFO("This moment [{}] is not the time to execute drop task: {}", Timer::now_s(), task.serialize_to_json());
    }
    return false;
  }
  return true;
}

void DropTaskHandler::execute(Task& task, BinlogEntry& instance)
{
  auto* task_param = (DropInstanceTaskParam*)task.task_param();
  bool drop_all = task_param->drop_all();

  OMS_INFO("Submit a task(id: {}) to drop binlog instance: {}", task.serialize_to_json(), instance.instance_name());
  g_executor->submit(
      binlog::InstanceAsynTasks::drop_binlog_instance, instance.instance_name(), task.task_id(), drop_all);
}

void DropTaskHandler::interrupt_recover(Task& task, BinlogEntry& instance)
{
  OMS_INFO("Begin to recover [drop instance] task for instance: {}", instance.serialize_to_json());
  if (InstanceState::DROP == instance.state()) {
    task.set_status(TaskStatus::COMPLETED);
    return;
  }

  task.set_status(TaskStatus::WAITING);
}

void StartTaskHandler::execute(Task& task, BinlogEntry& instance)
{
  if ((TaskStatus::WAITING == task.status() &&
          (instance.state() != InstanceState::STOP && instance.state() != InstanceState::FAILED &&
              instance.state() != InstanceState::OFFLINE))) {
    OMS_ERROR("May be dirty data, since the instance [{}] status expected by [recover instance] task [{}](state: "
              "{}) is [offline] rather than [{}]",
        instance.instance_name(),
        task.task_id(),
        task_status_str(task.status()),
        instance_state_str(instance.state()));
    return;
  }

  instance.set_state(binlog::InstanceState::STARTING);
  if (OMS_OK != binlog::g_cluster->update_instance(instance)) {
    OMS_ERROR("Failed to update binlog instance [{}] state to [starting]", instance.instance_name());
    return;
  }

  auto* task_param = (StartInstanceTaskParam*)task.task_param();
  uint64_t flag = task_param->flag();

  OMS_INFO("Submit a task(id: {}) to start binlog instance: {} with flag: {}",
      task.task_id(),
      instance.instance_name(),
      flag);
  g_executor->submit(binlog::InstanceAsynTasks::start_binlog_instance,
      instance.instance_name(),
      static_cast<hsql::InstanceFlag>(flag),
      task.task_id());
}

void StartTaskHandler::interrupt_recover(Task& task, BinlogEntry& instance)
{
  OMS_INFO("Begin to recover [start instance] task for instance: {}", instance.serialize_to_json());
  switch (instance.state()) {
    case InstanceState::STOP:
      task.set_status(TaskStatus::WAITING);
      break;
    case InstanceState::RUNNING:
    case InstanceState::GRAYSCALE:
      task.set_status(TaskStatus::COMPLETED);
      break;
    case InstanceState::FAILED:
      task.set_status(TaskStatus::FAILED);
      break;
    case InstanceState::STARTING:
      recover_starting_instance(task, instance);
      break;
    default:
      OMS_ERROR("Unsupported instance status with start task in executing status: {}", instance.state());
  }
}

void StopTaskHandler::execute(Task& task, BinlogEntry& instance)
{
  auto* task_param = (StopInstanceTaskParam*)task.task_param();
  uint64_t flag = task_param->flag();

  OMS_INFO("Submit a task(id: {}) to stop binlog instance: {} with flag: {}",
      task.task_id(),
      instance.instance_name(),
      flag);
  g_executor->submit(binlog::InstanceAsynTasks::stop_binlog_instance,
      instance.instance_name(),
      static_cast<hsql::InstanceFlag>(flag),
      task.task_id());
}

void StopTaskHandler::interrupt_recover(Task& task, BinlogEntry& instance)
{
  OMS_INFO("Begin to recover [stop instance] task for instance: {}", instance.serialize_to_json());
  if (InstanceState::STOP == instance.state()) {
    task.set_status(TaskStatus::COMPLETED);
    return;
  }

  task.set_status(TaskStatus::WAITING);
}

void RecoverTaskHandler::execute(Task& task, BinlogEntry& instance)
{
  if ((TaskStatus::WAITING == task.status() && instance.state() != InstanceState::OFFLINE &&
          instance.state() != InstanceState::INIT)) {
    OMS_ERROR("May be dirty data, since the instance [{}] status expected by [recover instance] task [{}](state: "
              "{}) is [Offline or Init] rather than [{}]",
        instance.instance_name(),
        task.task_id(),
        task_status_str(task.status()),
        instance_state_str(instance.state()));
    return;
  }
  instance.set_state(binlog::InstanceState::STARTING);
  if (OMS_OK != binlog::g_cluster->update_instance(instance)) {
    OMS_ERROR("Failed to update binlog instance [{}] state to [starting]", instance.instance_name());
    return;
  }

  auto* task_param = (RecoverInstanceTaskParam*)task.task_param();
  std::string cluster = task_param->cluster();
  std::string tenant = task_param->tenant();
  std::string offline_instance = task_param->offline_instance();

  OMS_INFO("Submit a task(id: {}) to recover binlog instance: {}", task.task_id(), offline_instance);
  g_executor->submit(binlog::InstanceAsynTasks::create_binlog_instance,
      cluster,
      tenant,
      instance.instance_name(),
      task.task_id(),
      true);
}

void RecoverTaskHandler::interrupt_recover(Task& task, BinlogEntry& instance)
{
  OMS_INFO("Begin to recover [recover instance] task for instance: {}", instance.serialize_to_json());
  switch (instance.state()) {
    case InstanceState::INIT:
    case InstanceState::OFFLINE:
      task.set_status(TaskStatus::WAITING);
      break;
    case InstanceState::RUNNING:
    case InstanceState::GRAYSCALE:
      task.set_status(TaskStatus::COMPLETED);
      break;
    case InstanceState::FAILED:
      task.set_status(TaskStatus::FAILED);
      break;
    case InstanceState::STARTING:
      recover_starting_instance(task, instance);
      break;
    default:
      OMS_ERROR("Unsupported instance status with recover task in executing status: {}", instance.state());
  }
}

int get_port_by_pid(int pid)
{
  std::ifstream file("/proc/" + std::to_string(pid) + "/cmdline");
  if (!file.is_open()) {
    return -1;
  }

  defer(file.close());
  std::vector<std::string> args;
  std::string arg;
  while (getline(file, arg, '\0')) {
    args.emplace_back(arg);
  }

  return args.empty() ? -1 : std::atoi(args.back().c_str());
}

void TaskHandler::recover_starting_instance(Task& task, BinlogEntry& instance)
{
  int pid = -1;
  int port = 0;
  bool is_exist = true;
  bool is_process_running = InstanceAsynTasks::is_work_path_occupied(instance.work_path(), is_exist);
  if (is_process_running) {
    std::string content;
    FsUtil::read_file(get_instance_pid_file(instance.work_path()), content, true);
    pid = std::atoi(content.c_str());
    port = get_port_by_pid(pid);
  }

  OMS_INFO("Recover starting instance, process running: {}, pid: {}, port: {}", is_process_running, pid, port);
  if (port > 0 && pid > 0) {
    instance.set_pid(pid);
    instance.set_port(port);
    instance.set_state(InstanceState::RUNNING);
    task.set_status(TaskStatus::COMPLETED);
  } else {
    instance.set_state(InstanceState::FAILED);
    task.set_status(TaskStatus::FAILED);
  }
}

}  // namespace oceanbase::binlog
