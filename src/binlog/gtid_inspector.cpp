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

#include "gtid_inspector.h"

#include <metric/prometheus.h>

namespace oceanbase::binlog {

void GtidConsistencyInspector::run()
{
  OMS_INFO("Start gtid consistency inspector with period(s): {}", s_config.gtid_inspector_s.val());
  while (is_run()) {
    std::vector<InstanceGtidSeq> instance_gtid_seq_vec;
    if (OMS_OK != g_cluster->get_gtid_seq_checkpoint(instance_gtid_seq_vec)) {
      OMS_ERROR("Failed to get gtid seq checkpoint.");
      continue;
    }

    std::map<std::string, std::set<std::string>> tenant_instance_map;
    std::set<std::string> inconsistent_tenants;
    bool passed = check_consistency(instance_gtid_seq_vec, tenant_instance_map, inconsistent_tenants);
    std::string tenant_instance_str;
    for (const auto& tenant : tenant_instance_map) {
      if (passed || inconsistent_tenants.find(tenant.first) != inconsistent_tenants.end()) {
        tenant_instance_str += tenant.first + ":";
        for (const auto& instance : tenant.second) {
          tenant_instance_str += instance + ",";
        }
        tenant_instance_str += "; ";
      }
    }

    if (passed) {
      OMS_INFO("Gtid consistency check passed for [{}] tenants: {}", tenant_instance_map.size(), tenant_instance_str);
    } else {
      OMS_ERROR("Gtid consistency check failed for [{}] tenants: {}", inconsistent_tenants.size(), tenant_instance_str);
    }
    _timer.sleep(s_config.gtid_inspector_s.val() * 1000000);
  }

  OMS_INFO("Gtid consistency inspector stopped.");
}

bool GtidConsistencyInspector::check_consistency(std::vector<InstanceGtidSeq>& instance_gtid_seq_vec,
    std::map<std::string, std::set<std::string>>& tenant_instance_map, std::set<std::string>& inconsistent_tenants)
{
  bool passed = true;
  std::map<std::string, std::map<uint64_t, InstanceGtidSeq>> tenant_gtid_checkpoint_map;
  for (const auto& instance_gtid_seq : instance_gtid_seq_vec) {
    std::string tenant = instance_gtid_seq.cluster() + "." + instance_gtid_seq.tenant();
    tenant_instance_map[tenant].insert(instance_gtid_seq.instance_name());
    if (tenant_gtid_checkpoint_map.find(tenant) == tenant_gtid_checkpoint_map.end() ||
        tenant_gtid_checkpoint_map[tenant].find(instance_gtid_seq.gtid_start()) ==
            tenant_gtid_checkpoint_map[tenant].end()) {
      tenant_gtid_checkpoint_map[tenant].emplace(instance_gtid_seq.gtid_start(), instance_gtid_seq);
      continue;
    }

    InstanceGtidSeq base_gtid_seq = tenant_gtid_checkpoint_map[tenant][instance_gtid_seq.gtid_start()];
    if (strcmp(base_gtid_seq.xid_start().c_str(), instance_gtid_seq.xid_start().c_str()) != 0) {
      passed = false;
      inconsistent_tenants.insert(tenant);
      OMS_ERROR("Multiple instances of the tenant [{}] may have inconsistent gtid <-> xid mapping: \n{}\n{}",
          tenant,
          base_gtid_seq.serialize_to_json(),
          instance_gtid_seq.serialize_to_json());
      PrometheusExposer::mark_binlog_counter_metric("",
          base_gtid_seq.instance_name(),
          base_gtid_seq.cluster(),
          base_gtid_seq.tenant(),
          base_gtid_seq.cluster_id(),
          base_gtid_seq.tenant_id(),
          BINLOG_INSTANCE_GTID_INCONSISTENT_COUNT_TYPE);
    }
  }
  return passed;
}

}  // namespace oceanbase::binlog
