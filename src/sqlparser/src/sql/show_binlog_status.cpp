//
// Created by 花轻 on 2023/6/13.
//

#include "show_binlog_status.h"
namespace hsql {

ShowBinlogStatusStatement::ShowBinlogStatusStatement() : SQLStatement(COM_SHOW_BINLOG_STAT), tenant(nullptr)
{}

ShowBinlogStatusStatement::~ShowBinlogStatusStatement()
{
  if (tenant) {
    delete tenant;
  }
}
}  // namespace hsql