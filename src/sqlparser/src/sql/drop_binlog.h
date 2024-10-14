//
// Created by 花轻 on 2023/6/13.
//

#pragma once
#include "SQLStatement.h"

// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {

struct DropBinlogStatement : SQLStatement {
  DropBinlogStatement();
  ~DropBinlogStatement() override;

  bool if_exists;
  TenantName* tenant_info;
  Expr* defer_execution_sec;
};

}  // namespace hsql
