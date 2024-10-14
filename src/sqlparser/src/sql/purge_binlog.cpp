//
// Created by 花轻 on 2023/6/13.
//

#include "purge_binlog.h"
// Note: Implementations of constructors and destructors can be found in statements.cpp.
namespace hsql {

PurgeBinlogStatement::PurgeBinlogStatement()
    : SQLStatement(COM_PURGE_BINLOG), tenant(nullptr), binlog_file(nullptr), purge_ts(nullptr), instance_name(nullptr)
{}

PurgeBinlogStatement::~PurgeBinlogStatement()
{
  if (tenant) {
    delete tenant;
  }
  if (nullptr == instance_name) {
    delete instance_name;
  }
  if (binlog_file) {
    delete binlog_file;
  }

  if (purge_ts) {
    delete purge_ts;
  }
}
}  // namespace hsql