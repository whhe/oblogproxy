//
// Created by 花轻 on 2023/6/13.
//
#pragma once
#include "SQLStatement.h"

// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {

struct ShowMasterStatusStatement : SQLStatement {
  ShowMasterStatusStatement();
  ~ShowMasterStatusStatement() override;
};

}  // namespace hsql
