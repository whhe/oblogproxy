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

#include "binlog_ops_stmt.h"

namespace hsql {

ReportStatement::ReportStatement() : SQLStatement(COM_REPORT), timestamp(nullptr)
{}

ReportStatement::~ReportStatement()
{
  if (nullptr != timestamp) {
    delete timestamp;
  }
}

ShowProcessListStatement::ShowProcessListStatement() : SQLStatement(COM_SHOW_PROCESS_LIST), instance_name(nullptr)
{}

ShowProcessListStatement::~ShowProcessListStatement()
{
  free(instance_name);
  instance_name = nullptr;
}

ShowDumpListStatement::ShowDumpListStatement() : SQLStatement(COM_SHOW_DUMP_LIST), instance_name(nullptr)
{}

ShowDumpListStatement::~ShowDumpListStatement()
{
  free(instance_name);
  instance_name = nullptr;
}

KillStatement::KillStatement() : SQLStatement(COM_KILL), process_id(nullptr), instance_name(nullptr)
{}

KillStatement::~KillStatement()
{
  if (nullptr != process_id) {
    delete process_id;
    process_id = nullptr;
  }
  free(instance_name);
  instance_name = nullptr;
}
}  // namespace hsql
