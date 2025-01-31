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

#pragma once

#include <map>

#ifdef _OBLOG_MSG_
#include "LogRecord.h"
using namespace oceanbase::logmessage;
#else
#include "BR.h"

#include "binlog_buf.h"
#include "meta_info.h"
#include "str_array.h"
#include "logmsg_buf.h"

using ILogRecord = IBinlogRecord;
using LogRecordImpl = BinlogRecordImpl;
#endif

static std::map<int, std::string> LOGGABLE_RECORD_TYPES = {
    {EINSERT, "insert"}, {EUPDATE, "update"}, {EDELETE, "delete"}, {EREPLACE, "replace"}, {EDDL, "ddl"}};
