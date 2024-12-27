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

#include <string>
#include <cassert>

#include <csignal>
#include "shell_executor.h"
#include "config.h"
#include "fs_util.h"
#include "str.h"
#include "sys_metric.h"
#include "guard.hpp"

#include <timer.h>

/**
 * FIXME... MACOS compatitable
 */
namespace oceanbase {
namespace logproxy {
#define CPU_START_POS 13
#define READ_BUF_SIZE 512
#define DATA_DIR "/data/"

// The maximum value of cgoup memory limit, in KB. Greater than or equal to this value means that docker has no limit on
// memory
static const int64_t MEM_DEFAULT = 9223372036854771712;
static const int64_t UNIT_GB = 1024 * 1024 * 1024L;
static const int64_t UNIT_MB = 1024 * 1024L;
static const int64_t UNIT_KB = 1024L;

Network _last_net_st;

static int get_cpu_core_count()
{
  const std::string& filename = "/sys/fs/cgroup/cpuacct/cpuacct.usage_percpu";
  std::string core_str;
  if (!FsUtil::read_file(filename, core_str) || core_str.empty()) {
    OMS_STREAM_ERROR << "Failed to get cpu core count for failed to read: " << filename;
    return 0;
  }
  std::vector<std::string> parts;
  split_by_str(core_str, " ", parts);
  return parts.size();
}

static int get_limit_cpu_core_count_from_cpuset()
{
  const std::string& filename = "/sys/fs/cgroup/cpuset/cpuset.cpus";
  std::string core_str;
  if (!FsUtil::read_file(filename, core_str)) {
    OMS_STREAM_ERROR << "Failed to get_limit_cpu_core_count_from_cpuset for failed to read:" << filename;
    return 0;
  }

  std::vector<std::string> parts;
  split_by_str(core_str, ",", parts);

  int core_num = 0;
  for (const std::string& part : parts) {
    std::vector<std::string> core_part;
    split_by_str(part, "-", core_part);
    if (core_part.empty()) {
      return 0;
    }

    if (core_part.size() == 1) {
      core_num++;
      continue;
    }

    if (core_part.size() > 2) {
      return 0;
    }
    core_num += abs(atoi(core_part[0].c_str()) - atoi(core_part[1].c_str())) + 1;
  }
  return core_num;
}

static int64_t get_limit_cpu_core_count()
{
  int64_t quota = 0;
  if (!FsUtil::read_number("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", quota)) {
    OMS_STREAM_ERROR << "Failed to read cpu.cfs_quota_us";
    return 0;
  }
  if (quota == -1) {
    return get_limit_cpu_core_count_from_cpuset();
  }

  int64_t period = 0;
  bool ret = FsUtil::read_number("/sys/fs/cgroup/cpu/cpu.cfs_period_us", period);
  if (!ret || period <= 0) {
    OMS_STREAM_ERROR << "Failed to read cfs_period_us";
    return 0;
  }
  return quota / period;
}

/*
 * @description get the CPU time slice used by the operating system in the current state
 * @date 2022/9/19 16:20
 */
static int64_t get_sys_cpu_usage()
{
  std::ifstream proc_stat("/proc/stat");
  if (!proc_stat.good()) {
    OMS_ERROR("Failed to open file:{}", "/proc/stat");
  }
  std::string line;
  std::getline(proc_stat, line);
  proc_stat.close();

  std::istringstream iss(line);
  std::string cpu;
  long long user, nice, system, idle;

  iss >> cpu >> user >> nice >> system >> idle;
  return user + nice + system + idle;
}

static int64_t get_cpu_tick()
{
  std::string result;
  int ret = exec_cmd("getconf CLK_TCK", result);
  if (ret != 0 || result.empty()) {
    OMS_STREAM_ERROR << "Failed to get cpu tick for failed to exec cmd: getconf CLK_TCK";
    return 0;
  }
  return atoll(result.c_str());
}

static int64_t get_cpu_tick_nano(int64_t tick)
{
  return 1000 * 1000 * 1000 / tick;
}

bool get_cpu_usage(int64_t& cpu_usage)
{
  cpu_usage = 0;
  int ret = FsUtil::read_number("/sys/fs/cgroup/cpuacct/cpuacct.usage", cpu_usage);
  if (!ret || cpu_usage <= 0) {
    OMS_STREAM_ERROR << "Failed to collect cpu for failed to read cpuacct.usage again";
    return false;
  }
  return true;
}

bool collect_cpu(Cpu* cpu_status)
{
  Timer timer;
  int cpu_core_count = get_cpu_core_count();
  if (cpu_core_count <= 0) {
    OMS_STREAM_ERROR << "Failed to collect cpu for failed to get_cpu_core_count";
    return false;
  }
  int64_t limit_cpu_core_count = get_limit_cpu_core_count();
  if (limit_cpu_core_count <= 0) {
    OMS_STREAM_ERROR << "Failed to collect cpu for failed to get_limit_cpu_core_count";
    return false;
  }

  int64_t cpu_usage_pre = 0;
  if (!get_cpu_usage(cpu_usage_pre)) {
    return false;
  }

  int64_t sys_cpu_usage_pre = get_sys_cpu_usage();
  if (sys_cpu_usage_pre < 0) {
    OMS_STREAM_ERROR << "Failed to collect cpu for failed to failed to get_sys_cpu_usage";
    return false;
  }
  timer.sleep(500 * 1000);
  int64_t cpu_usage;
  if (!get_cpu_usage(cpu_usage)) {
    return false;
  }
  int64_t sys_cpu_usage = get_sys_cpu_usage();
  if (sys_cpu_usage < 0) {
    OMS_STREAM_ERROR << "Failed to collect cpu for failed to failed to get_sys_cpu_usage again";
    return false;
  }

  int64_t cpu_tick = get_cpu_tick();
  if (cpu_tick <= 0) {
    OMS_STREAM_ERROR << "Failed to collect cpu for failed to get_cpu_tick";
    return false;
  }
  int64_t sys_cpu_delta = (sys_cpu_usage - sys_cpu_usage_pre) * get_cpu_tick_nano(cpu_tick);
  if (sys_cpu_delta > 1) {
    int64_t cpu_delta = cpu_usage - cpu_usage_pre;
    float cpu_usage_ratio = static_cast<float>(cpu_delta) / static_cast<float>(sys_cpu_delta) *
                            static_cast<float>(cpu_core_count) / static_cast<float>(limit_cpu_core_count);
    cpu_status->set_cpu_used_ratio(cpu_usage_ratio);
  }
  cpu_status->set_cpu_count(limit_cpu_core_count);
  return true;
}

bool collect_load(Load* load_status)
{
  std::string result;
  if (!FsUtil::read_file("/proc/loadavg", result) || result.empty()) {
    OMS_STREAM_ERROR << "Failed to collect load for failed to read /proc/loadavg";
    return false;
  }
  std::vector<std::string> load_items;
  split_by_str(result, " ", load_items);
  if (load_items.size() >= 3) {
    load_status->set_load_1(atof(load_items[0].c_str()));
    load_status->set_load_5(atof(load_items[1].c_str()));
    load_status->set_load_15(atof(load_items[2].c_str()));
  }
  return true;
}

static int64_t get_mem_total()
{
  std::string result;
  int ret = exec_cmd("cat /proc/meminfo | grep 'MemTotal' | awk '{print $(NF-1)}'", result);
  if (ret != 0 || result.empty()) {
    return MEM_DEFAULT;
  }
  return atoll(result.c_str());
}

bool collect_mem(Memory* memory_status)
{
  uint64_t mem_usage_bytes = 0;
  std::map<std::string, std::string> mem_stat;
  if (!FsUtil::read_kvs("/sys/fs/cgroup/memory/memory.stat", " ", mem_stat)) {
    OMS_STREAM_ERROR << "Failed to collect memory for failed to read memory.stat";
    return false;
  }

  uint64_t total_rss = 0;
  auto entry = mem_stat.find("total_rss");
  if (entry != mem_stat.end()) {
    total_rss = atoll(entry->second.c_str());
  }
  uint64_t total_mapped_file = 0;
  entry = mem_stat.find("total_mapped_file");
  if (entry != mem_stat.end()) {
    total_mapped_file = atoll(entry->second.c_str());
  }

  mem_usage_bytes = total_rss + total_mapped_file;

  int64_t mem_limit = 0;
  if (!FsUtil::read_number("/sys/fs/cgroup/memory/memory.limit_in_bytes", mem_limit)) {
    OMS_STREAM_ERROR << "Failed to collect memory for failed to read memory.limit_in_bytes";
    return false;
  }
  if (mem_limit >= MEM_DEFAULT) {
    mem_limit = get_mem_total() * 1024;
  }

  memory_status->set_mem_used_ratio((float)mem_usage_bytes / (float)mem_limit);
  memory_status->set_mem_total_size_mb(mem_limit / UNIT_MB);
  memory_status->set_mem_used_size_mb(mem_usage_bytes / UNIT_MB);
  return true;
}

bool collect_disk(Disk* disk_status, const std::string& path)
{
  uint64_t folder_size_bytes = FsUtil::folder_size(path);
  disk_status->set_disk_usage_size_process_mb(folder_size_bytes / UNIT_MB);
  FsUtil::disk_info disk_info = FsUtil::space(path);

  disk_status->set_disk_total_size_mb(disk_info.capacity / UNIT_MB);
  disk_status->set_disk_used_size_mb((disk_info.capacity - disk_info.available) / UNIT_MB);
  if (disk_status->disk_total_size_mb() != 0) {
    disk_status->set_disk_used_ratio(
        (float)disk_status->disk_used_size_mb() / (float)disk_status->disk_total_size_mb());
  }
  return true;
}

bool collect_pro_disk(Disk* disk_status, const std::string& path)
{
  uint64_t folder_size_bytes = FsUtil::folder_size(path);
  disk_status->set_disk_usage_size_process_mb(folder_size_bytes / UNIT_MB);
  FsUtil::disk_info disk_info = FsUtil::space(path);

  disk_status->set_disk_total_size_mb(disk_info.capacity / UNIT_MB);
  disk_status->set_disk_used_size_mb((disk_info.capacity - disk_info.available) / UNIT_MB);
  if (disk_status->disk_total_size_mb() != 0) {
    disk_status->set_disk_used_ratio(
        (float)disk_status->disk_usage_size_process_mb() / (float)disk_status->disk_total_size_mb());
  }
  return true;
}

void get_total_recv_transmit_bytes(std::ifstream& file, uint64_t& recv_bytes, uint64_t& transmit_bytes)
{
  std::string line;
  getline(file, line);
  getline(file, line);

  uint64_t total_recv_bytes = 0;
  uint64_t total_transmit_bytes = 0;
  while (getline(file, line)) {
    std::stringstream ss(line);
    std::string iface, recv_bytes_str, recv_packets, recv_errs, recv_drop, recv_fifo, recv_frame, recv_compressed,
        recv_multicast;
    std::string transmit_bytes_str, transmit_packets, transmit_errs, transmit_drop, transmit_fifo, transmit_colls,
        transmit_carrier, transmit_compressed;

    ss >> iface >> recv_bytes_str >> recv_packets >> recv_errs >> recv_drop >> recv_fifo >> recv_frame >>
        recv_compressed >> recv_multicast >> transmit_bytes_str >> transmit_packets >> transmit_errs >> transmit_drop >>
        transmit_fifo >> transmit_colls >> transmit_carrier >> transmit_compressed;

    total_recv_bytes += atoll(recv_bytes_str.c_str());
    total_transmit_bytes += atoll(transmit_bytes_str.c_str());
  }
  recv_bytes = total_recv_bytes;
  transmit_bytes = total_transmit_bytes;
}

void get_network_stat(Network& network_status)
{
  std::ifstream file("/proc/net/dev");
  if (file.bad()) {
    OMS_DEBUG("Failed to open file:{}", "/proc/net/dev");
    return;
  }
  uint64_t recv_bytes = 0;
  uint64_t transmit_bytes = 0;
  get_total_recv_transmit_bytes(file, recv_bytes, transmit_bytes);
  network_status.set_network_rx_bytes(recv_bytes);
  network_status.set_network_wx_bytes(transmit_bytes);
}

void get_pro_network_stat(uint64_t pid, Network& network_status)
{
  char filename[1024] = {};
  sprintf(filename, "/proc/%lu/net/dev", pid);
  std::ifstream file(filename);
  if (file.bad()) {
    OMS_DEBUG("Failed to open file:{}", filename);
    return;
  }
  uint64_t recv_bytes = 0;
  uint64_t transmit_bytes = 0;
  get_total_recv_transmit_bytes(file, recv_bytes, transmit_bytes);
  network_status.set_network_rx_bytes(recv_bytes);
  network_status.set_network_wx_bytes(transmit_bytes);
}

void collect_network(Network* network_status)
{
  Network net_st;
  if (_last_net_st.network_rx_bytes() == 0 && _last_net_st.network_wx_bytes() == 0) {
    // first round, reset
    get_network_stat(net_st);
    _last_net_st.set_network_rx_bytes(net_st.network_rx_bytes());
    _last_net_st.set_network_wx_bytes(net_st.network_wx_bytes());
    network_status->set_network_rx_bytes(0);
    network_status->set_network_wx_bytes(0);
  }

  get_network_stat(net_st);
  network_status->set_network_rx_bytes(
      (net_st.network_rx_bytes() - _last_net_st.network_rx_bytes()) / Config::instance().metric_interval_s.val());
  network_status->set_network_wx_bytes(
      (net_st.network_wx_bytes() - _last_net_st.network_wx_bytes()) / Config::instance().metric_interval_s.val());
  _last_net_st.set_network_rx_bytes(net_st.network_rx_bytes());
  _last_net_st.set_network_wx_bytes(net_st.network_wx_bytes());
}

int64_t get_pro_cpu_time(uint64_t pid)
{
  char filename[1024] = {};
  sprintf(filename, "/proc/%lu/stat", pid);
  std::string content;
  if (!FsUtil::read_file(filename, content)) {
    OMS_DEBUG("Failed to read stat:{}", filename);
    return 0;
  }
  std::vector<std::string> cpu_stat;
  split(content, ' ', cpu_stat);

  if (cpu_stat.size() < CPU_START_POS + 4) {
    return 0;
  }
  return atol(cpu_stat.at(CPU_START_POS).c_str()) + atol(cpu_stat.at(CPU_START_POS + 1).c_str()) +
         atol(cpu_stat.at(CPU_START_POS + 2).c_str()) + atol(cpu_stat.at(CPU_START_POS + 3).c_str());
}

void set_cpu(std::map<uint64_t, int64_t> s_pre_pro_cpu, std::map<uint64_t, int64_t> s_cur_pro_cpu,
    uint64_t limit_cpu_core, long s_pre_sys_cpu, long s_cur_sys_cpu,
    std::pair<const unsigned long, ProcessMetric*> process)
{
  if ((s_cur_pro_cpu[process.first] == s_pre_pro_cpu[process.first]) || (s_cur_sys_cpu == s_pre_sys_cpu) ||
      (s_cur_pro_cpu[process.first] == 0) || (s_cur_sys_cpu == 0)) {
    process.second->cpu_status()->set_cpu_used_ratio(0);
    process.second->cpu_status()->set_cpu_count(limit_cpu_core);
  } else {
    auto ratio =
        (float(s_cur_pro_cpu[process.first] - s_pre_pro_cpu[process.first])) / float(s_cur_sys_cpu - s_pre_sys_cpu);
    process.second->cpu_status()->set_cpu_used_ratio(ratio);
    process.second->cpu_status()->set_cpu_count(limit_cpu_core);
  }
}
void set_net(std::map<uint64_t, Network> s_pre_pro_net, std::map<uint64_t, Network> s_cur_pro_net,
    std::pair<const unsigned long, ProcessMetric*> process)
{
  process.second->network_status()->set_network_rx_bytes(
      (s_cur_pro_net[process.first].network_rx_bytes() - s_pre_pro_net[process.first].network_rx_bytes()));
  process.second->network_status()->set_network_wx_bytes(
      (s_cur_pro_net[process.first].network_wx_bytes() - s_pre_pro_net[process.first].network_wx_bytes()));
}
void batch_get_pro_stat(std::map<uint64_t, ProcessMetric*>& process_map)
{
  std::map<uint64_t, int64_t> s_pre_pro_cpu;
  std::map<uint64_t, int64_t> s_cur_pro_cpu;
  std::map<uint64_t, Network> s_pre_pro_net;
  std::map<uint64_t, Network> s_cur_pro_net;
  uint64_t limit_cpu_core = get_limit_cpu_core_count();
  for (auto process : process_map) {
    s_pre_pro_cpu[process.first] = get_pro_cpu_time(process.first);
    get_pro_network_stat(process.first, s_pre_pro_net[process.first]);
  }
  long s_pre_sys_cpu = get_sys_cpu_usage();
  sleep(1);
  for (auto process : process_map) {
    s_cur_pro_cpu[process.first] = get_pro_cpu_time(process.first);
    get_pro_network_stat(process.first, s_cur_pro_net[process.first]);
  }
  long s_cur_sys_cpu = get_sys_cpu_usage();

  for (auto process : process_map) {
    set_cpu(s_pre_pro_cpu, s_cur_pro_cpu, limit_cpu_core, s_pre_sys_cpu, s_cur_sys_cpu, process);
    set_net(s_pre_pro_net, s_cur_pro_net, process);
  }
}

uint64_t get_process_file_handle_count(uint64_t pid)
{
  DIR* dir;
  struct dirent* entry;
  uint64_t count = 0;
  std::string fd_path = "/proc/" + std::to_string(pid) + "/fd";

  dir = opendir(fd_path.c_str());
  if (dir) {
    while ((entry = readdir(dir))) {
      if (entry->d_type == DT_LNK) {
        count++;
      }
    }
    closedir(dir);
  }
  return count;
}

ProcessMetric* find_proc_metric(ProcessGroupMetric* proc_group_metric, int pid);

void get_mem_stat(std::map<std::string, std::string> status, ProcessMetric* process_metric)
{
  if (status.find("VmRSS:") != status.end()) {
    std::string vmrss_str = status.find("VmRSS:")->second;
    logproxy::trim(vmrss_str);
    std::vector<std::string> vmrss_parts;
    logproxy::split(vmrss_str, ' ', vmrss_parts);
    if (!vmrss_parts.empty()) {
      uint64_t vmrss = std::stoul(vmrss_parts.at(0));
      process_metric->memory_status()->set_mem_used_size_mb(vmrss / 1024L);
      process_metric->memory_status()->set_mem_total_size_mb(get_mem_total() / 1024L);
      process_metric->memory_status()->set_mem_used_ratio(double(process_metric->memory_status()->mem_used_size_mb()) /
                                                          double(process_metric->memory_status()->mem_total_size_mb()));
    }
  }
}

void collect_by_pid_name(const std::string& pid_name, ProcessGroupMetric* proc_group_metric)
{
  const struct dirent* ent;
  char filename[READ_BUF_SIZE] = {};

  DIR* proc_dir = opendir("/proc");
  assert(proc_dir != nullptr);
  /*
   * The processmetric object here does not need to be released
   */
  std::map<uint64_t, ProcessMetric*> process_map;

  while ((ent = readdir(proc_dir)) != nullptr) {
    if (strcmp(ent->d_name, "..") == 0) {
      continue;
    }

    if (!isdigit(*ent->d_name)) {
      continue;
    }

    sprintf(filename, "/proc/%s/status", ent->d_name);

    std::map<std::string, std::string> status;
    if (!FsUtil::read_kvs(filename, "\t", status)) {
      continue;
    }

    if (status.find("Name:") == status.end()) {
      continue;
    }

    std::string proc_name = status.find("Name:")->second;
    if (proc_name != pid_name) {
      continue;
    }

    uint64_t pid = std::stoull(ent->d_name);
    // Path to build symbolic links
    char link_path[PATH_MAX] = {};
    std::snprintf(link_path, sizeof(link_path), "/proc/%lu/cwd", pid);
    // collect disk
    // Read the target path pointed to by the symbolic link
    char cwd[PATH_MAX] = {};
    if (readlink(link_path, cwd, sizeof(cwd)) == -1) {
      continue;
    }
    ProcessMetric* process_metric = nullptr;
    std::string path = std::string(cwd);
    process_metric = find_proc_metric(proc_group_metric, pid);
    bool is_update = true;
    if (process_metric == nullptr) {
      process_metric = new ProcessMetric(true);
      process_metric->set_pid(pid);
      is_update = false;
    }
    process_map.emplace(pid, process_metric);

    // collect mem
    get_mem_stat(status, process_metric);
    // collect disk
    /*!
     * @brief Binlog mode collects resource indicators of non-main processes
     */
    if (Config::instance().binlog_mode.val() && proc_name != "logproxy") {
      collect_pro_disk(process_metric->disk_status(), path + DATA_DIR);
    } else {
      collect_pro_disk(process_metric->disk_status(), path);
    }
    process_metric->set_client_id(path);
    process_metric->set_fd_count(get_process_file_handle_count(pid));
    if (!is_update) {
      proc_group_metric->metric_group().push_back(process_metric);
    }
  }

  batch_get_pro_stat(process_map);
  closedir(proc_dir);
}

ProcessMetric* find_proc_metric(ProcessGroupMetric* proc_group_metric, int pid)
{
  for (auto* p_metric : proc_group_metric->metric_group().get_items()) {
    auto p_process_metric = (ProcessMetric*)p_metric;
    if (p_process_metric->pid() == pid) {
      return p_process_metric;
    }
  }
  return nullptr;
}

bool collect_metric(SysMetric* metric)
{
  OMS_DEBUG("Start collecting:{}", metric->serialize_to_json());
  collect_network(metric->network_status());

  collect_load(metric->load_status());

  collect_mem(metric->memory_status());

  collect_cpu(metric->cpu_status());

  std::error_code err;
  std::string path = std::filesystem::current_path(err);
  if (!err) {
    collect_disk(metric->disk_status(), path);
  }
  collect_process_metric(metric->process_group_metric());
  OMS_DEBUG("End collecting:{}", metric->serialize_to_json());
  return true;
}

void clean_up_expired_indicators(ProcessGroupMetric* proc_group_metric)
{
  std::unique_lock<std::mutex> lock(g_metric_mutex);
  // Clean up dead process indicators
  for (auto iterator = proc_group_metric->metric_group().get_items().begin();
       iterator != proc_group_metric->metric_group().get_items().end();) {
    auto* p_process_metric = static_cast<ProcessMetric*>(*iterator);
    if (p_process_metric != nullptr && 0 != kill(p_process_metric->pid(), 0)) {
      OMS_DEBUG("The current process {} is no longer alive,pid:{},remove the process from indicator collection",
          p_process_metric->client_id(),
          p_process_metric->pid());
      delete *iterator;
      *iterator = nullptr;
      OMS_DEBUG("Remove the process from indicator collection");
      iterator = proc_group_metric->metric_group().get_items().erase(iterator);
    } else {
      ++iterator;
    }
  }
}
/*
 * @params
 * @returns Whether the collection is successful
 * @description Collect all resource metric whose process names are logproxy, logreader and binlog
 * @date 2022/12/4 20:40
 */
bool collect_process_metric(ProcessGroupMetric* proc_group_metric)
{
  clean_up_expired_indicators(proc_group_metric);
  for (const std::string& proc_name : proc_group_metric->item_names) {
    collect_by_pid_name(proc_name, proc_group_metric);
  }
  return true;
}

SysMetric::SysMetric(bool initialize)
{
  if (initialize) {
    auto* cpu = new Cpu();
    auto* load = new Load();
    auto* memory = new Memory();
    auto* disk = new Disk();
    auto* network = new Network();
    auto* process_metric = new ProcessMetric();
    auto* process = new ProcessGroupMetric();
    /*!
     * A value needs to be filled in by default TODO implement reflection
     */
    process->metric_group().push_back(process_metric);
    this->set_memory_status(memory);
    this->set_cpu_status(cpu);
    this->set_disk_status(disk);
    this->set_network_status(network);
    this->set_load_status(load);
    this->set_process_group_metric(process);
  }
}

ProcessMetric::ProcessMetric(bool initialized)
{
  if (initialized) {
    auto* cpu = new Cpu();
    auto* memory = new Memory();
    auto* disk = new Disk();
    auto* network = new Network();
    this->set_memory_status(memory);
    this->set_cpu_status(cpu);
    this->set_disk_status(disk);
    this->set_network_status(network);
  }
}

SysMetric* g_metric = new SysMetric();

}  // namespace logproxy
}  // namespace oceanbase
