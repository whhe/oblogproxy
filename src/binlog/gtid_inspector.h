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

#include "thread.h"
#include "timer.h"
#include "env.h"

namespace oceanbase::binlog {
using namespace oceanbase::logproxy;

class GtidConsistencyInspector : public Thread {
public:
  GtidConsistencyInspector() : Thread("GtidConsistencyInspector")
  {}

  ~GtidConsistencyInspector() override = default;

public:
  void run() override;

  static bool check_consistency(std::vector<InstanceGtidSeq>& instance_gtid_seq_vec,
      std::map<std::string, std::set<std::string>>& tenant_instance_map, std::set<std::string>& inconsistent_tenants);

private:
  Timer _timer;
};

}  // namespace oceanbase::binlog
