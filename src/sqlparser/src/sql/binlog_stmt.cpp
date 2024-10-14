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

#include "binlog_stmt.h"

namespace hsql {

hsql::CreateBinlogStatement::CreateBinlogStatement()
    : SQLStatement(COM_CREATE_BINLOG), ts(nullptr), tenant(nullptr), binlog_options(nullptr)
{}

hsql::CreateBinlogStatement::~CreateBinlogStatement()
{
  if (ts) {
    delete ts;
  }

  if (tenant) {
    delete tenant;
  }

  if (user_info) {
    delete user_info;
  }

  if (binlog_options) {
    for (SetClause* option : *binlog_options) {
      delete option;
    }
    delete binlog_options;
  }
}

AlterBinlogStatement::AlterBinlogStatement()
    : SQLStatement(COM_ALTER_BINLOG), tenant(nullptr), instance_options(nullptr)
{}

hsql::AlterBinlogStatement::~AlterBinlogStatement()
{
  delete tenant;
  if (nullptr != instance_options) {
    for (SetClause* clause : *instance_options) {
      if (nullptr != clause->column) {
        free(clause->column);
      }
      if (nullptr != clause->value) {
        delete clause->value;
      }
      delete clause;
    }
    delete instance_options;
  }
}

}  // namespace hsql