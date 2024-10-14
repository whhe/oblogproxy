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

struct ReportStatement : SQLStatement {
  ReportStatement();
  ~ReportStatement() override;

  Expr* timestamp;
};

struct ShowProcessListStatement : SQLStatement {
  ShowProcessListStatement();
  ~ShowProcessListStatement() override;

  bool full = false;
  char* instance_name;
};

struct ShowDumpListStatement : SQLStatement {
  ShowDumpListStatement();
  ~ShowDumpListStatement() override;

  bool full = false;
  char* instance_name;
};

struct KillStatement : SQLStatement {
  KillStatement();
  ~KillStatement() override;

  // true: kill query, false: kill connection(default)
  bool only_kill_query = false;
  Expr* process_id;
  char* instance_name;
};

}  // namespace hsql
