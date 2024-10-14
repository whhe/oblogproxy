//
// Created by 花轻 on 2023/6/14.
//

#include "show_slave_status.h"
namespace hsql {
ShowSlaveStatusStatement::ShowSlaveStatusStatement() : SQLStatement(COM_SHOW_SLAVE_STAT)
{}

ShowSlaveStatusStatement::~ShowSlaveStatusStatement()
{}
}  // namespace hsql