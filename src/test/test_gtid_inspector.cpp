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

#include "gtest/gtest.h"

#include "gtid_inspector.h"

using namespace oceanbase::binlog;
using namespace oceanbase::logproxy;

std::string instance1 = "{\"cluster\":\"binlogit\",\"commit_version_start\":1702535185,\"gtid_start\":200,\"instance_"
                        "name\":\"111\",\"tenant\":\"ob_mysql\",\"trxs_num\":1,\"xid_start\":\"1002_69393\"}";
std::string instance2 = "{\"cluster\":\"binlogit\",\"commit_version_start\":1702535185,\"gtid_start\":200,\"instance_"
                        "name\":\"222\",\"tenant\":\"ob_mysql\",\"trxs_num\":1,\"xid_start\":\"1002_6993\"}";
std::string instance3 = "{\"cluster\":\"binlogit\",\"commit_version_start\":1702535185,\"gtid_start\":200,\"instance_"
                        "name\":\"333\",\"tenant\":\"ob_mysql\",\"trxs_num\":1,\"xid_start\":\"1002_69903\"}";

std::string instance6 = "{\"cluster\":\"binlogit\",\"commit_version_start\":1702535185,\"gtid_start\":200,\"instance_"
                        "name\":\"111\",\"tenant\":\"ob_mysql1\",\"trxs_num\":1,\"xid_start\":\"1002_6993\"}";

void add_instance_gtid(const std::string& str, std::vector<InstanceGtidSeq>& instance_gtid_seq_vec)
{
  InstanceGtidSeq gtid_seq_1;
  gtid_seq_1.deserialize_from_json(str);
  instance_gtid_seq_vec.emplace_back(gtid_seq_1);
}

TEST(GtidConsistencyInspector, check_consistency)
{
  std::map<std::string, std::set<std::string>> tenant_instance_map;
  std::set<std::string> inconsistent_tenants;
  std::vector<InstanceGtidSeq> instance_gtid_seq_vec;
  add_instance_gtid(instance1, instance_gtid_seq_vec);
  add_instance_gtid(instance2, instance_gtid_seq_vec);
  add_instance_gtid(instance3, instance_gtid_seq_vec);
  add_instance_gtid(instance6, instance_gtid_seq_vec);
  GtidConsistencyInspector::check_consistency(instance_gtid_seq_vec, tenant_instance_map, inconsistent_tenants);
}