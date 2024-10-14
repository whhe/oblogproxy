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

#include "log.h"
#include "cluster/node.h"

namespace oceanbase::binlog {
using namespace oceanbase::logproxy;

class Filter {
public:
  explicit Filter(std::string name) : _name(std::move(name))
  {}

  virtual ~Filter() = default;

public:
  std::string name()
  {
    return _name;
  }

  virtual bool can_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes) = 0;

  virtual int do_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes, std::string& err_msg) = 0;

private:
  std::string _name;
};

class IpFilter : public Filter {
public:
  IpFilter() : Filter("IpFilter")
  {}

  ~IpFilter() override = default;

public:
  bool can_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes) override;

  int do_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes, std::string& err_msg) override;
};

class ZoneFilter : public Filter {
public:
  ZoneFilter() : Filter("ZoneFilter")
  {}

  ~ZoneFilter() override = default;

public:
  bool can_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes) override;

  int do_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes, std::string& err_msg) override;
};

class GroupFilter : public Filter {
public:
  GroupFilter() : Filter("GroupFilter")
  {}

  ~GroupFilter() override = default;

public:
  bool can_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes) override;

  int do_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes, std::string& err_msg) override;
};

class RegionFilter : public Filter {
public:
  RegionFilter() : Filter("RegionFilter")
  {}

  ~RegionFilter() override = default;

public:
  bool can_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes) override;

  int do_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes, std::string& err_msg) override;

private:
  static void remove_instances_in_drop(std::vector<BinlogEntry*>& instance_entries);
};

class ResourcesFilter : public Filter {
public:
  ResourcesFilter() : Filter("ResourcesFilter")
  {}

  ~ResourcesFilter() override = default;

public:
  bool can_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes) override;

  int do_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes, std::string& err_msg) override;

private:
  static bool meet_resource_needs(InstanceMeta& meta, logproxy::Node& node);
};

class WeightedRandomFilter : public Filter {
public:
  WeightedRandomFilter() : Filter("WeightedRandomFilter")
  {}

  ~WeightedRandomFilter() override = default;

public:
  bool can_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes) override;

  int do_filter(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes, std::string& err_msg) override;

private:
  static float weighted_nodes(vector<logproxy::Node*>& nodes, std::map<std::string, float>& node_id_weight_mapping);

  static std::string weighted_selection(std::map<std::string, float>& node_id_weight_mapping, float total_weight);
};

using InstanceWitGtid = std::vector<std::pair<BinlogEntry, std::pair<InstanceGtidSeq, InstanceGtidSeq>>>;
class SelectionStrategy {
public:
  static int load_balancing_node(InstanceMeta& meta, Node& node, std::string& err_msg);

  static int load_balancing_node(InstanceMeta& meta, std::vector<logproxy::Node*>& nodes, std::string& err_msg);

  static int select_master_instance(std::vector<BinlogEntry>& running_instances, uint64_t minimal_checkpoint,
      BinlogEntry& election_instance, std::string& err_msg);

  static void filter_instance_by_initial_point(
      uint64_t start_timestamp, std::vector<BinlogEntry*>& instances, BinlogEntry& selected_instance);

private:
  static void filter_master_by_min_checkpoint(InstanceWitGtid& instance_vec, uint64_t minimal_checkpoint);

  static void filter_master_by_resource(InstanceWitGtid& instance_vec);

  static void filter_master_by_delay(InstanceWitGtid& instance_vec, uint64_t minimal_checkpoint);
};

}  // namespace oceanbase::binlog
