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

#include "node.h"

#include "obaccess/ob_access.h"
#include "str.h"

namespace oceanbase::logproxy {

std::string node_state_print(uint16_t code)
{
  static const char* _s_node_state_names[] = {"ONLINE", "SUSPECT", "OFFLINE", "PAUSED", "GRAYSCALE"};
  if (code <= InstanceState::GRAYSCALE) {
    return _s_node_state_names[code];
  }
  return "UNDEFINED";
}

bool BinlogEntry::equal(const BinlogEntry& entry)
{
  return strcmp(entry.instance_name().c_str(), instance_name().c_str()) == 0;
}

ConfigTemplate::ConfigTemplate(bool initialize)
{}

PrimaryInstance::PrimaryInstance(bool initialize)
{}

Node::Node(bool initialize)
{
  if (initialize) {
    auto* metric = new SysMetric(true);
    this->set_metric(metric);
  }
}

NodeConfig::NodeConfig(bool initialize)
{}

BinlogEntry::BinlogEntry(bool initialize)
{
  if (initialize) {
    auto meta = new InstanceMeta(true);
    this->set_config(meta);
  }
}

std::string BinlogEntry::full_tenant() const
{
  return cluster() + "." + tenant();
}

void BinlogEntry::update_initial_gtid(const InstanceGtidSeq& gtid_seq)
{
  config()->cdc_config()->set_start_timestamp(gtid_seq.commit_version_start());
  config()->binlog_config()->set_initial_ob_txn_id(gtid_seq.xid_start());
  uint64_t txn_gtid = gtid_seq.xid_start().empty() ? gtid_seq.gtid_start() + 1 : gtid_seq.gtid_start();
  config()->binlog_config()->set_initial_ob_txn_gtid_seq(txn_gtid);
}

void BinlogEntry::update_initial_gtid(const BinlogEntry& entry)
{
  config()->cdc_config()->set_start_timestamp(entry.config()->cdc_config()->start_timestamp());
  config()->binlog_config()->set_initial_ob_txn_id(entry.config()->binlog_config()->initial_ob_txn_id());
  config()->binlog_config()->set_initial_ob_txn_gtid_seq(entry.config()->binlog_config()->initial_ob_txn_gtid_seq());
}

void BinlogEntry::init_instance(Node& node, InstanceMeta* meta)
{
  this->set_node_id(node.id());
  this->set_ip(node.ip());
  this->set_instance_name(meta->instance_name());
  this->set_cluster(meta->cluster());
  this->set_tenant(meta->tenant());
  this->set_state(InstanceState::INIT);
  this->set_config(meta);
  this->set_cluster_id(meta->cluster_id());
  this->set_tenant_id(meta->tenant_id());
}

InstanceGtidSeq::InstanceGtidSeq(bool initialize)
{}

void deserialize_gtid_seq_str(
    const BinlogEntry& instance, const std::string& gtid_seq_str, std::vector<InstanceGtidSeq>& gtid_seq_vec)
{
  std::vector<std::string> gtid_seq_str_vec;
  split(gtid_seq_str, '|', gtid_seq_str_vec);
  for (auto each_gtid_gtid : gtid_seq_str_vec) {
    std::vector<std::string> filed_set;
    split(each_gtid_gtid, '#', filed_set);
    if (filed_set.size() == 4) {
      InstanceGtidSeq gtid_seq;
      gtid_seq.set_cluster(instance.cluster());
      gtid_seq.set_tenant(instance.tenant());
      gtid_seq.set_instance_name(instance.instance_name());
      gtid_seq.set_commit_version_start(std::atoll(filed_set.at(0).c_str()));
      gtid_seq.set_xid_start(filed_set.at(1));
      gtid_seq.set_gtid_start(std::atoll(filed_set.at(2).c_str()));
      gtid_seq.set_trxs_num(std::atoll(filed_set.at(3).c_str()));
      gtid_seq.set_cluster_id(instance.cluster_id());
      gtid_seq.set_tenant_id(instance.tenant_id());
      gtid_seq_vec.emplace_back(gtid_seq);
    } else {
      OMS_WARN("Failed to deserialize gtid seq: {}, set size: {}", each_gtid_gtid, filed_set.size());
    }
  }
}

User::User(bool initialize)
{}

}  // namespace oceanbase::logproxy
