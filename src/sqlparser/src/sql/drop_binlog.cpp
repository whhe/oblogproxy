//
// Created by 花轻 on 2023/6/13.
//

#include "drop_binlog.h"
namespace hsql {
DropBinlogStatement::DropBinlogStatement()
    : SQLStatement(COM_DROP_BINLOG), tenant_info(nullptr), defer_execution_sec(nullptr)
{}

DropBinlogStatement::~DropBinlogStatement()
{
  if (tenant_info) {
    delete tenant_info;
  }
  if (nullptr != defer_execution_sec) {
    delete defer_execution_sec;
    defer_execution_sec = nullptr;
  }
}
}  // namespace hsql
