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

#include <cstdint>
#include "thread.h"
#include "fs_util.h"

namespace oceanbase::binlog {

class InstanceSocketListener : public logproxy::Thread {
public:
  explicit InstanceSocketListener(uint64_t tcp_port);

  ~InstanceSocketListener();

  void run() override;

  int try_lock();

private:
  int write_pid_file(int pid);

private:
  uint64_t _listen_port;
  FILE* _file;
};

}  // namespace oceanbase::binlog
