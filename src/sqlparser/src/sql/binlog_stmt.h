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
#include "SQLStatement.h"

namespace hsql {
struct CreateBinlogStatement : SQLStatement {
  CreateBinlogStatement();
  ~CreateBinlogStatement() override;

  TenantName* tenant;
  bool is_for_not_exists;
  Expr* ts;
  std::vector<SetClause*>* binlog_options;

  // specified sys account password
  UserInfo* user_info;
};

struct AlterBinlogStatement : SQLStatement {
  AlterBinlogStatement();
  ~AlterBinlogStatement() override;

  TenantName* tenant;
  std::vector<SetClause*>* instance_options;
};

}  // namespace hsql
