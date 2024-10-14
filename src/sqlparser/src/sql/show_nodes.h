//
// Created by 花轻 on 2023/11/23.
//

#pragma once
#include "SQLStatement.h"

// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {

enum ShowNodeOption { NODE_IP, NODE_ZONE, NODE_REGION, NODE_GROUP, NODE_ALL, NODE_UNKNOWN };

struct ShowNodesStatement : SQLStatement {
  ShowNodesStatement();

  ~ShowNodesStatement() override;

  ShowNodeOption option;
  char* ip;
  char* zone;
  char* region;
  char* group;
};

}  // namespace hsql
