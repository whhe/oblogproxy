//
// Created by 花轻 on 2023/11/23.
//

#include "show_nodes.h"

// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {
ShowNodesStatement::ShowNodesStatement() : SQLStatement(COM_SHOW_NODES)
{}

ShowNodesStatement::~ShowNodesStatement()
{}
}  // namespace hsql
