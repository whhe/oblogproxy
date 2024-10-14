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

#include "selection_strategy.h"

#include <set>
#include "env.h"
#include "str.h"

#include <metric/prometheus.h>

namespace oceanbase::binlog {

static IpFilter _s_ip_filter;
static ZoneFilter _s_zone_filter;
static GroupFilter _s_group_filter;
static RegionFilter _s_region_filter;
static ResourcesFilter _s_resource_filter;
static WeightedRandomFilter _s_weighted_random_filter;
static std::vector<Filter*> _s_load_balancing_filters{&_s_ip_filter,
    &_s_zone_filter,
    &_s_group_filter,
    &_s_resource_filter,
    &_s_region_filter,
    &_s_weighted_random_filter};

std::function<std::string(std::string, float)> kv_gen = [](const std::string& key, float value) {
  return key + ":" + std::to_string(value);
};

int SelectionStrategy::load_balancing_node(InstanceMeta& meta, Node& node, std::string& err_msg)
{
  std::vector<Node*> nodes;
  std::set<State> node_states;
  node_states.insert(State::ONLINE);
  if (OMS_OK != g_cluster->query_nodes(nodes, node_states)) {
    err_msg = "Failed to get nodes in cluster";
    OMS_ERROR(err_msg);
    return OMS_FAILED;
  }
  defer(release_vector(nodes));

  int ret = load_balancing_node(meta, nodes, err_msg);
  if (OMS_OK != ret || nodes.empty()) {
    OMS_ERROR(err_msg);
    return OMS_FAILED;
  }
  node = *(nodes.at(0));
  return OMS_OK;
}

int SelectionStrategy::load_balancing_node(InstanceMeta& meta, std::vector<Node*>& nodes, std::string& err_msg)
{
  if (nodes.empty()) {
    err_msg = "No nodes in running status";
    OMS_ERROR(err_msg);
    return OMS_FAILED;
  }

  std::function<std::string(Node*)> func = [](Node* node) { return node->identifier(); };
  OMS_INFO("Begin to select load balancing node for instance [{}] from nodes [{}]: {}",
      meta.instance_name(),
      nodes.size(),
      join_vector_str(nodes, func));
  int ret = OMS_OK;
  for (Filter* filter : _s_load_balancing_filters) {
    if (OMS_OK != ret) {
      OMS_INFO("No longer call load balancing filters after {}(including), last filter ret: {}", filter->name(), ret);
      PrometheusExposer::mark_binlog_counter_metric("",
          meta.instance_name(),
          meta.cluster(),
          meta.tenant(),
          meta.cluster_id(),
          meta.tenant_id(),
          BINLOG_ALLOCATE_NODE_FAIL_COUNT_TYPE);
      break;
    }

    if (filter->can_filter(meta, nodes)) {
      ret = filter->do_filter(meta, nodes, err_msg);
      OMS_INFO(
          "{}: remaining [{}] nodes after filtering: {}", filter->name(), nodes.size(), join_vector_str(nodes, func));
    } else {
      OMS_INFO("{}: skip and remaining nodes num [{}]: {}", filter->name(), nodes.size(), join_vector_str(nodes, func));
    }
  }
  return ret;
}

int SelectionStrategy::select_master_instance(std::vector<BinlogEntry>& running_instances, uint64_t min_dump_checkpoint,
    BinlogEntry& election_instance, std::string& err_msg)
{
  OMS_INFO("Select master instance from instances in running status [{}] for tenant [{}] to meet minimal dump "
           "checkpoint: {}",
      join_vector_str(running_instances, instance_name_gen),
      running_instances.front().full_tenant(),
      min_dump_checkpoint);
  InstanceWitGtid instance_vec;
  for (const auto& instance : running_instances) {
    std::pair<InstanceGtidSeq, InstanceGtidSeq> gtid_seq_pair;
    g_cluster->boundary_gtid_seq(instance.instance_name(), gtid_seq_pair);
    instance_vec.emplace_back(instance, gtid_seq_pair);
  }

  // 1. filter by minimal checkpoint
  filter_master_by_min_checkpoint(instance_vec, min_dump_checkpoint);
  if (instance_vec.empty()) {
    err_msg = "All instances in running status do not meet the min dump checkpoint requirement";
    OMS_ERROR(err_msg);
    return OMS_FAILED;
  }
  OMS_INFO("Total [{}] instances meet minimal dump checkpoint [{}]", instance_vec.size(), min_dump_checkpoint);

  // 2. filter by resources requirements
  filter_master_by_resource(instance_vec);
  if (instance_vec.empty()) {
    err_msg = "All instances in running status for tenant do not have enough resources to provide service.";
    OMS_ERROR(err_msg);
    return OMS_FAILED;
  }
  OMS_INFO("Total [{}] instances meet resource requirement to provide service, resource check: {}",
      instance_vec.size(),
      s_config.enable_resource_check.val());

  // 3. try to select the instance with real-time binlog conversion
  filter_master_by_delay(instance_vec, min_dump_checkpoint);
  OMS_INFO("Remain total [{}] instances after filtering by delay.", instance_vec.size());

  // 4. select instance with minimal checkpoint
  uint64_t min_checkpoint = UINT64_MAX;
  for (const auto& instance : instance_vec) {
    if (instance.second.second.commit_version_start() != 0 &&
        min_checkpoint > instance.second.second.commit_version_start()) {
      min_checkpoint = instance.second.second.commit_version_start();
      election_instance = instance.first;
    }
  }
  election_instance = instance_vec.front().first;
  OMS_INFO("Select instance [{}] with the minimal binlog storage checkpoint as service provider for tenant [{}]",
      election_instance.instance_name(),
      election_instance.full_tenant());
  return OMS_OK;
}

void SelectionStrategy::filter_instance_by_initial_point(
    uint64_t start_timestamp, std::vector<BinlogEntry*>& instances, BinlogEntry& selected_instance)
{
  OMS_INFO("Begin to select base instance according to timestamp [{}] from [{}] instances: {}",
      start_timestamp,
      instances.size(),
      join_vector_str(instances, p_instance_name_gen));
  int64_t less_min_interval = INT64_MAX;
  BinlogEntry less_than_instance;
  int64_t greater_min_interval = INT64_MIN;
  BinlogEntry greater_than_instance;
  // filter out instance whose start timestamp is closest to the given start_timestamp
  for (auto instance : instances) {
    int64_t interval = start_timestamp - instance->config()->cdc_config()->start_timestamp();
    if (interval >= 0 && interval < less_min_interval) {
      less_min_interval = interval;
      less_than_instance = *(instance);
    }
    if (interval < 0 && interval > greater_min_interval) {
      greater_min_interval = interval;
      greater_than_instance = *(instance);
    }
  }

  OMS_INFO("The instances than closest to timestamp [{}], less than: {}, greater than: {}.",
      start_timestamp,
      less_than_instance.instance_name(),
      greater_than_instance.instance_name());
  // prefer instance with start timestamp <= given start_timestamp
  if (!less_than_instance.instance_name().empty()) {
    selected_instance = less_than_instance;
    return;
  }
  if (!greater_than_instance.instance_name().empty()) {
    selected_instance = greater_than_instance;
    return;
  }
  selected_instance = *(instances.at(0));
}

void SelectionStrategy::filter_master_by_min_checkpoint(InstanceWitGtid& instance_vec, uint64_t minimal_checkpoint)
{
  if (0 == minimal_checkpoint) {
    return;
  }
  for (auto iter = instance_vec.begin(); iter != instance_vec.end();) {
    if (iter->second.first.commit_version_start() == 0 ||
        minimal_checkpoint < iter->second.first.commit_version_start()) {
      OMS_WARN("The smallest checkpoint of instance [{}] > minimal checkpoint: {} > {}",
          iter->first.instance_name(),
          iter->second.first.commit_version_start(),
          minimal_checkpoint);
      iter = instance_vec.erase(iter);
    } else {
      iter++;
    }
  }
}

void SelectionStrategy::filter_master_by_resource(InstanceWitGtid& instance_vec)
{
  if (!s_config.enable_resource_check.val()) {
    return;
  }
  double node_cpu_ratio_limit = s_config.node_cpu_limit_threshold_percent.val() * 0.01;
  for (auto iter = instance_vec.begin(); iter != instance_vec.end();) {
    Node node;
    g_cluster->query_node_by_id(iter->first.node_id(), node);
    if (node.metric()->cpu_status()->cpu_used_ratio() > node_cpu_ratio_limit) {
      OMS_WARN("The instance [{}] does not meet resource requirement due the cpu used of node [{}]: {} >= {}",
          iter->first.instance_name(),
          node.identifier(),
          node.metric()->cpu_status()->cpu_used_ratio(),
          node_cpu_ratio_limit);
      iter = instance_vec.erase(iter);
    } else {
      iter++;
    }
  }
}

void SelectionStrategy::filter_master_by_delay(InstanceWitGtid& instance_vec, uint64_t min_dump_checkpoint)
{
  std::vector<std::string> instances_with_delay;
  uint64_t now_s = Timer::now_s();
  uint64_t delay = now_s > min_dump_checkpoint ? now_s - min_dump_checkpoint : 0;
  uint64_t threshold = std::min(delay, (uint64_t)300);
  for (const auto& instance : instance_vec) {
    if (instance.second.second.commit_version_start() < now_s &&
        (now_s - instance.second.second.commit_version_start() > threshold)) {
      OMS_WARN("The delay of instance [{}] exceeds delay threshold: {} - {} > {}",
          instance.first.instance_name(),
          now_s,
          instance.second.second.commit_version_start(),
          threshold);
      instances_with_delay.emplace_back(instance.first.instance_name());
    }
  }

  OMS_INFO("The num of instances that exceed delay(s) [{}]: {}", threshold, instances_with_delay.size());
  if (instances_with_delay.empty()) {
    return;
  }
  if (instances_with_delay.size() == instance_vec.size()) {
    OMS_WARN("All Instances does not meet delay requirement: {}", threshold);
    return;
  }

  for (auto iter = instance_vec.begin(); iter != instance_vec.end();) {
    if (std::find(instances_with_delay.begin(), instances_with_delay.end(), iter->first.instance_name()) !=
        instances_with_delay.end()) {
      iter = instance_vec.erase(iter);
    } else {
      iter++;
    }
  }
}

bool IpFilter::can_filter(InstanceMeta& meta, vector<logproxy::Node*>& nodes)
{
  return !nodes.empty() && !meta.slot_config()->ip().empty();
}

int IpFilter::do_filter(InstanceMeta& meta, vector<logproxy::Node*>& nodes, std::string& err_msg)
{
  std::string ip = meta.slot_config()->ip();
  OMS_INFO("Begin to filter node for instance [{}] according to the specified ip: {}", meta.instance_name(), ip);

  for (auto iter = nodes.begin(); iter != nodes.end();) {
    if (strcmp(ip.c_str(), (*iter)->ip().c_str()) != 0) {
      delete *iter;
      iter = nodes.erase(iter);
    } else {
      iter++;
    }
  }
  if (nodes.empty()) {
    err_msg = "Not matched any node with ip: " + ip;
    OMS_ERROR(err_msg);
    return OMS_FAILED;
  }
  return OMS_OK;
}

bool ZoneFilter::can_filter(InstanceMeta& meta, vector<logproxy::Node*>& nodes)
{
  return !nodes.empty() && !meta.slot_config()->zone().empty();
}

int ZoneFilter::do_filter(InstanceMeta& meta, vector<logproxy::Node*>& nodes, string& err_msg)
{
  std::string zone = meta.slot_config()->zone();
  OMS_INFO("[ZoneFilter] Begin to filter node for instance [{}] according to the specified zone: {}",
      meta.instance_name(),
      zone);

  for (auto iter = nodes.begin(); iter != nodes.end();) {
    if (strcmp(zone.c_str(), (*iter)->zone().c_str()) != 0) {
      delete *iter;
      iter = nodes.erase(iter);
    } else {
      iter++;
    }
  }
  if (nodes.empty()) {
    err_msg = "Not matched any node with zone: " + zone;
    OMS_ERROR(err_msg);
    return OMS_FAILED;
  }
  return OMS_OK;
}

bool GroupFilter::can_filter(InstanceMeta& meta, vector<logproxy::Node*>& nodes)
{
  return !nodes.empty() && !meta.slot_config()->group().empty();
}

int GroupFilter::do_filter(InstanceMeta& meta, vector<logproxy::Node*>& nodes, std::string& err_msg)
{
  std::string group = meta.slot_config()->group();
  OMS_INFO("[GroupFilter] Begin to filter node for instance [{}] according to the specified group: {}",
      meta.instance_name(),
      group);

  for (auto iter = nodes.begin(); iter != nodes.end();) {
    if (strcmp(group.c_str(), (*iter)->group().c_str()) != 0) {
      delete *iter;
      iter = nodes.erase(iter);
    } else {
      iter++;
    }
  }
  if (nodes.empty()) {
    err_msg = "Not matched any node with group: " + group;
    OMS_ERROR(err_msg);
    return OMS_FAILED;
  }
  return OMS_OK;
}

bool RegionFilter::can_filter(InstanceMeta& meta, vector<logproxy::Node*>& nodes)
{
  return nodes.size() > 1;
}

int RegionFilter::do_filter(InstanceMeta& meta, vector<logproxy::Node*>& nodes, std::string& err_msg)
{
  std::string region = meta.slot_config()->region();
  // 1. region specified by the creating sql
  if (!region.empty()) {
    OMS_INFO("[RegionFilter] Begin to filter node for instance [{}] according to the specified region: {}",
        meta.instance_name(),
        region);

    for (auto iter = nodes.begin(); iter != nodes.end();) {
      if (strcmp(region.c_str(), (*iter)->region().c_str()) != 0) {
        delete *iter;
        iter = nodes.erase(iter);
      } else {
        iter++;
      }
    }
    if (nodes.empty()) {
      err_msg = "Not matched any node with region: " + region;
      OMS_ERROR(err_msg);
      return OMS_FAILED;
    }
    return OMS_OK;
  }

  /*
   * 2. when region is not specified, follow the steps below:
   * 1) select nodes according to region without instances for tenant
   * 2) select nodes according to ip without instances for tenant
   * 3) select all nodes
   */
  std::vector<BinlogEntry*> instance_entries;
  defer(release_vector(instance_entries));
  BinlogEntry condition;
  condition.set_cluster(meta.cluster());
  condition.set_tenant(meta.tenant());
  if (OMS_OK != g_cluster->query_instances(instance_entries, condition)) {
    OMS_ERROR("[RegionFilter] Failed to query instances for tenant: {}.{}", meta.cluster(), meta.tenant());
    release_vector(instance_entries);
    return OMS_FAILED;
  }
  remove_instances_in_drop(instance_entries);
  OMS_INFO("[RegionFilter] Found [{}] existed binlog instances for tenant: {}.{}",
      instance_entries.size(),
      meta.cluster(),
      meta.tenant());

  if (!instance_entries.empty()) {
    std::set<std::string> all_regions;
    std::set<std::string> all_nodes;
    std::map<std::string, std::string> ip_region_mapping;
    for (auto node : nodes) {
      all_regions.insert(node->region());
      all_nodes.insert(node->id());
      ip_region_mapping[node->ip()] = node->region();
    }
    std::set<std::string> regions_for_existing_instances;
    std::set<std::string> nodes_for_existing_instances;
    for (auto entry : instance_entries) {
      regions_for_existing_instances.insert(ip_region_mapping[entry->ip()]);
      nodes_for_existing_instances.insert(entry->node_id());
    }

    std::vector<std::string> available_regions;
    std::set_difference(all_regions.begin(),
        all_regions.end(),
        regions_for_existing_instances.begin(),
        regions_for_existing_instances.end(),
        std::back_inserter(available_regions),
        [](const std::string& a, const std::string& b) { return a < b; });

    if (!available_regions.empty()) {
      // 1. filter out nodes in available regions without instances for tenant
      std::string regions_str;
      for (const std::string& str : available_regions) {
        regions_str += str + ",";
      }
      OMS_INFO("[RegionFilter] Filter out nodes according to available regions [{}] without instances for tenant "
               "[{}.{}] yet",
          regions_str,
          meta.cluster(),
          meta.tenant());
      for (auto iter = nodes.begin(); iter != nodes.end();) {
        if (std::find(available_regions.begin(), available_regions.end(), (*iter)->region()) ==
            available_regions.end()) {
          delete *iter;
          iter = nodes.erase(iter);
        } else {
          iter++;
        }
      }
    } else if (nodes_for_existing_instances.size() < all_nodes.size()) {
      // 2. filter out nodes without instances for tenant
      OMS_INFO("[RegionFilter] No available regions, and filter out nodes according to ip without instances(total: "
               "{}) for tenant [{}.{}] yet",
          all_nodes.size() - nodes_for_existing_instances.size(),
          meta.cluster(),
          meta.tenant());
      for (auto iter = nodes.begin(); iter != nodes.end();) {
        if (nodes_for_existing_instances.find((*iter)->id()) != nodes_for_existing_instances.end()) {
          delete *iter;
          iter = nodes.erase(iter);
        } else {
          iter++;
        }
      }
    }
  }
  // 3. do not filter nodes when no instance for tenant or each node has at least 1 instances
  return OMS_OK;
}

void RegionFilter::remove_instances_in_drop(std::vector<BinlogEntry*>& instance_entries)
{
  for (auto iter = instance_entries.begin(); iter != instance_entries.end();) {
    if ((*iter)->is_dropped()) {
      delete (*iter);
      iter = instance_entries.erase(iter);
    } else {
      iter++;
    }
  }
}

bool ResourcesFilter::can_filter(InstanceMeta& meta, vector<logproxy::Node*>& nodes)
{
  return !nodes.empty() && s_config.enable_resource_check.val();
}

int ResourcesFilter::do_filter(InstanceMeta& meta, vector<logproxy::Node*>& nodes, std::string& err_msg)
{
  for (auto iter = nodes.begin(); iter != nodes.end();) {
    if (!meet_resource_needs(meta, *(*iter))) {
      delete *iter;
      iter = nodes.erase(iter);
    } else {
      iter++;
    }
  }

  if (nodes.empty()) {
    err_msg = "Node resources are insufficient";
    OMS_ERROR(err_msg);
    return OMS_FAILED;
  }
  return OMS_OK;
}

bool ResourcesFilter::meet_resource_needs(InstanceMeta& meta, Node& node)
{
  double node_cpu_ratio_limit = s_config.node_cpu_limit_threshold_percent.val() * 0.01;
  double node_mem_ratio_limit = s_config.node_mem_limit_threshold_percent.val() * 0.01;
  double node_disk_ratio_limit = s_config.node_disk_limit_threshold_percent.val() * 0.01;

  // 1. used cpu < 0.2; used memory < 0.85; used disk < 0.7
  bool meet_required_threshold = node.metric()->cpu_status()->cpu_used_ratio() < node_cpu_ratio_limit &&
                                 node.metric()->memory_status()->mem_used_ratio() < node_mem_ratio_limit &&
                                 node.metric()->disk_status()->disk_used_ratio() < node_disk_ratio_limit;

  if (!meet_required_threshold) {
    OMS_ERROR("[ResourcesFilter] The resource threshold of node [{}] does not meet requirements, used cpu: {} >= {} "
              "|| used memory: {} >= {} || used disk: {} >= {}",
        node.identifier(),
        node.metric()->cpu_status()->cpu_used_ratio(),
        node_cpu_ratio_limit,
        node.metric()->memory_status()->mem_used_ratio(),
        node_mem_ratio_limit,
        node.metric()->disk_status()->disk_used_ratio(),
        node_disk_ratio_limit);
    return false;
  }

  // 2. available memory > obcdc memory limit && available disk > expire binlog log
  std::string memory_limit_str = meta.cdc_config()->memory_limit();
  uint32_t memory_limit_gb =
      memory_limit_str.empty() ? 20 : std::atoi(memory_limit_str.substr(0, memory_limit_str.size() - 1).c_str());
  uint32_t available_memory_gb =
      (node.metric()->memory_status()->mem_total_size_mb() - node.metric()->memory_status()->mem_used_size_mb()) / 1024;

  uint64_t binlog_size_gb = meta.binlog_config()->binlog_expire_logs_size() / 1024 / 1024 / 1024;
  uint64_t available_disk_gb =
      (node.metric()->disk_status()->disk_total_size_mb() - node.metric()->disk_status()->disk_used_size_mb()) / 1024;

  bool meet_minimum_required_resources = available_memory_gb >= memory_limit_gb && available_disk_gb >= binlog_size_gb;
  if (!meet_minimum_required_resources) {
    OMS_ERROR("[ResourcesFilter] Node [{}] does not meet minimum resources requirement, available memory(GB): {} < {} "
              "&& available "
              "disk(GB): {} < {}",
        node.identifier(),
        available_memory_gb,
        memory_limit_gb,
        available_disk_gb,
        binlog_size_gb);
  }
  return meet_minimum_required_resources;
}

bool WeightedRandomFilter::can_filter(InstanceMeta& meta, vector<logproxy::Node*>& nodes)
{
  return nodes.size() > 1;
}

int WeightedRandomFilter::do_filter(InstanceMeta& meta, vector<logproxy::Node*>& nodes, std::string& err_msg)
{
  std::map<std::string, float> node_id_weight_mapping;
  float total_weight = weighted_nodes(nodes, node_id_weight_mapping);

  std::string node_id = weighted_selection(node_id_weight_mapping, total_weight);
  for (auto iter = nodes.begin(); iter != nodes.end();) {
    if (strcmp(node_id.c_str(), (*iter)->id().c_str()) != 0) {
      delete *iter;
      iter = nodes.erase(iter);
    } else {
      iter++;
    }
  }
  return OMS_OK;
}

float WeightedRandomFilter::weighted_nodes(
    vector<logproxy::Node*>& nodes, std::map<std::string, float>& node_id_weight_mapping)
{
  // 1. get configuration weight
  std::string cpu_mem_disk_net_weighted_str = s_config.cpu_mem_disk_net_weighted.val();
  std::vector<std::string> cpu_mem_disk_net_weighted_vec;
  logproxy::split(cpu_mem_disk_net_weighted_str, ':', cpu_mem_disk_net_weighted_vec);
  assert(cpu_mem_disk_net_weighted_vec.size() == 4);

  std::vector<float> cpu_mem_disk_net_weighted(4);
  for (const auto& weight : cpu_mem_disk_net_weighted_vec) {
    cpu_mem_disk_net_weighted.emplace_back(std::atoi(weight.c_str()) * 1.0);
  }

  // 2. Calculate the weight of each node
  std::map<std::string, float> node_id_net_mapping;
  float total_net_kb = 0;
  for (auto& iter : nodes) {
    Node node = *iter;
    float available_cpu_ratio = (1 - node.metric()->cpu_status()->cpu_used_ratio());
    float available_mem_ratio = (1 - node.metric()->memory_status()->mem_used_ratio());
    float available_disk_ratio = (1 - node.metric()->disk_status()->disk_used_ratio());
    node_id_weight_mapping[node.id()] = cpu_mem_disk_net_weighted[0] * available_cpu_ratio +
                                        cpu_mem_disk_net_weighted[1] * available_mem_ratio +
                                        cpu_mem_disk_net_weighted[2] * available_disk_ratio;

    float net_kb =
        ((node.metric()->network_status()->network_rx_bytes() + node.metric()->network_status()->network_wx_bytes()) *
            1.0) /
        1024;
    node_id_net_mapping[node.id()] = net_kb;
    total_net_kb += net_kb;
  }

  float total_weight = 0;
  for (auto& id_weight : node_id_weight_mapping) {
    float available_net_ratio = (1 - (node_id_net_mapping[id_weight.first] / total_net_kb));
    id_weight.second += cpu_mem_disk_net_weighted[3] * available_net_ratio;
    total_weight += id_weight.second;
  }
  return total_weight;
}

std::string WeightedRandomFilter::weighted_selection(
    std::map<std::string, float>& node_id_weight_mapping, float total_weight)
{
  std::string node_id;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<float> dis(0.0, total_weight);
  float random = dis(gen);

  OMS_INFO("[WeightedRandomFilter] Weighted random selection, total: {}, random: {}，weight distribution: {}",
      total_weight,
      random,
      join_map_str(node_id_weight_mapping, kv_gen));
  for (const auto& weight : node_id_weight_mapping) {
    random = random - weight.second;
    if (random <= 0) {
      node_id = weight.first;
      break;
    }
  }

  if (node_id.empty()) {
    OMS_ERROR("No node was selected during weighted random selection and select first, random: {}", random);
    node_id = node_id_weight_mapping.begin()->first;
  }
  return node_id;
}

}  // namespace oceanbase::binlog
