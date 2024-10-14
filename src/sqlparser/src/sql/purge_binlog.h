//
// Created by 花轻 on 2023/6/13.
//

#pragma once
#include "SQLStatement.h"

// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {

struct PurgeBinlogStatement : SQLStatement {
  PurgeBinlogStatement();
  ~PurgeBinlogStatement() override;

  Expr* binlog_file;
  Expr* purge_ts;
  TenantName* tenant;
  char* instance_name;
};

}  // namespace hsql
