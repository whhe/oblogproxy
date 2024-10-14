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

#include "instance_socket_listener.h"

#include "env.h"
#include "file_lock.h"
#include "event_dispatch.h"

namespace oceanbase::binlog {

InstanceSocketListener::InstanceSocketListener(uint64_t tcp_port) : _listen_port(tcp_port), _file(nullptr)
{}

InstanceSocketListener::~InstanceSocketListener()
{
  if (nullptr != _file) {
    int fd = fileno(_file);
    OMS_PROC_UNLOCK(fd);
    FsUtil::fclose_binary(_file);
    _file = nullptr;
  }
}

void InstanceSocketListener::run()
{
  struct event_base* ev_base = event_base_new();
  // 1. enable tcp port
  int error_no = 0;
  struct evconnlistener* tcp_listener = evconnlistener_new(ev_base, _listen_port, on_new_obi_connection_cb, error_no);
  if (nullptr == tcp_listener) {
    OMS_ERROR("Failed to start OceanBase binlog instance on port: {}.", _listen_port);
    write_pid_file(-error_no);
    event_base_free(ev_base);
    return;
  }
  fcntl(evconnlistener_get_fd(tcp_listener), F_SETFD, FD_CLOEXEC);
  OMS_INFO("Start OceanBase binlog instance on port: {}.", _listen_port);

  // 2. enable uds
  struct evconnlistener* uds_listener = evconnlistener_new_uds(ev_base, on_new_obi_connection_cb);
  if (nullptr == uds_listener) {
    OMS_ERROR("Failed to start binlog instance on enable unix domain socket path: {}", INSTANCE_SOCKET_PATH);
    write_pid_file(-error_no);
    evconnlistener_free(tcp_listener);
    event_base_free(ev_base);
    return;
  }
  fcntl(evconnlistener_get_fd(uds_listener), F_SETFD, FD_CLOEXEC);
  OMS_INFO("Start OceanBase binlog instance on unix domain socket path: {}.", INSTANCE_SOCKET_PATH);

  if (OMS_OK != write_pid_file(getpid())) {
    return;
  }
  OMS_INFO("Binlog instance process pid: {}", getpid());

  event_base_dispatch(ev_base);
  OMS_INFO("Stop OceanBase binlog instance.");

  evconnlistener_free(tcp_listener);
  evconnlistener_free(uds_listener);
  event_base_free(ev_base);
}

int InstanceSocketListener::try_lock()
{
  _file = FsUtil::fopen_binary(INSTANCE_PID_FILE, "w+");
  if (nullptr == _file) {
    OMS_ERROR("Failed to open pid file: {}", INSTANCE_PID_FILE);
    return OMS_FAILED;
  }

  int fd = fileno(_file);
  if (-1 == OMS_PROC_NON_BLOCK_WRLOCK(fd)) {
    OMS_ERROR("Failed to lock pid file: {}", INSTANCE_PID_FILE);
    return OMS_FAILED;
  }
  OMS_INFO("Successfully lock current work path");
  return OMS_OK;
}

int InstanceSocketListener::write_pid_file(int pid)
{
  std::string pid_str = std::to_string(pid);
  int64_t ret = FsUtil::append_file(_file, pid_str, pid_str.size());
  if (OMS_FAILED == ret) {
    OMS_ERROR("Failed to write pid {} to file: {}", pid, INSTANCE_PID_FILE);
    return OMS_FAILED;
  }
  fflush(_file);
  return OMS_OK;
}

}  // namespace oceanbase::binlog
