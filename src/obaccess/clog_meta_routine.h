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

#include "thread.h"
#include "mysql_protocol.h"
#include "obcdc_config.h"
#include "ob_access.h"

namespace oceanbase {
namespace logproxy {
class ObLogReader;

class ClogMetaRoutine : public Thread {
public:
  void stop() override;

  int init(const ObcdcConfig& config);

  int fetch_once(uint64_t& min_clog_timestamp_us);

  bool check(uint64_t clog_timestamp_us);

  bool check(const ObcdcConfig& config, std::string& errmsg);

protected:
  void run() override;

private:
  bool _available = true;
  uint64_t _last_check_timestamp_us = 0;

  uint64_t _min_clog_timestamp_us = 0;
  MysqlProtocol _sys_auther;
  ObAccess _ob_access;
  ObcdcConfig _oblog_config{""};
};

}  // namespace logproxy
}  // namespace oceanbase
