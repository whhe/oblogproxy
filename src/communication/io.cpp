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

#include "sys/un.h"
#include <unistd.h>
#include <cerrno>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>

#include "communication/io.h"
#include "log.h"
#include "common.h"
#include "guard.hpp"

namespace oceanbase {
namespace logproxy {
int writen(int fd, const void* buf, int size)
{
  const char* tmp = (const char*)buf;
  while (size > 0) {
    //    OMS_STREAM_INFO << "about to write, fd: " << fd << ", size: " << size;
    const ssize_t ret = ::write(fd, tmp, size);
    //    OMS_STREAM_INFO << "done to write, fd:" << fd << ", size: " << size << ", ret:" << ret << ", errno:" << errno;
    if (ret >= 0) {
      tmp += ret;
      size -= ret;
      continue;
    }

    const int err = errno;
    if (EAGAIN != err && EINTR != err) {
      return OMS_FAILED;
    }
    /*
     * Currently there is a sending abnormality or network accumulation. need to wait for a while and try again.Prevent
     * busy-wait context switching from occupying too many CPU resources
     */
    usleep(1000);
  }
  return OMS_OK;
}

int readn(int fd, void* buf, int size)
{
  char* tmp = (char*)buf;
  int needed_size = size;
  while (size > 0) {
    //    OMS_STREAM_INFO << "about to read, fd: " << fd << ", size: " << size;
    const ssize_t ret = ::read(fd, tmp, size);
    //    OMS_STREAM_INFO << "done to read, fd:" << fd << ", size: " << size << ", ret:" << ret << ", errno:" << errno;
    if (ret > 0) {
      tmp += ret;
      size -= ret;
      continue;
    }

    if (0 == ret) {
      return size == needed_size ? OMS_CLIENT_CLOSED : OMS_FAILED;  // end of file
    }

    const int err = errno;
    if (EAGAIN != err && EINTR != err) {
      return OMS_FAILED;
    }
  }
  return OMS_OK;
}

int connect(const char* host, int port, bool block_mode, int timeout, int& sockfd)
{
  if (nullptr == host || 0 == host[0] || port < 0 || port >= 65536) {
    OMS_STREAM_ERROR << "Invalid host or port";
    return OMS_FAILED;
  }

  struct addrinfo hints;
  struct addrinfo* servinfo;
  int status;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  status = getaddrinfo(host, std::to_string(port).data(), &hints, &servinfo);
  if (status != 0) {
    OMS_ERROR("Failed to get host by name({}), error: {}", host, system_err(errno));
    return OMS_FAILED;
  }

  defer(freeaddrinfo(servinfo));
  const int sock = socket(servinfo->ai_family, servinfo->ai_socktype, servinfo->ai_protocol);
  if (sock == -1) {
    OMS_ERROR("Failed to create socket. error={}", strerror(errno));
    return OMS_FAILED;
  }

  int ret = OMS_OK;
  if (!block_mode) {
    ret = set_non_block(sock);
    if (ret != 0) {
      close(sock);
      return OMS_FAILED;
    }
  }

  std::string addr_str = std::string(host) + std::string(":") + std::to_string(port);
  ret = connect(sock, servinfo->ai_addr, servinfo->ai_addrlen);
  if (ret != 0) {
    if (!block_mode && (errno == EINPROGRESS || errno == EAGAIN)) {
      struct pollfd pollfd {};
      pollfd.fd = sock;
      pollfd.events = POLLERR | POLLHUP | POLLOUT;
      pollfd.revents = 0;

      ret = poll(&pollfd, 1, timeout);
      if (ret == 1 && (pollfd.revents & POLLOUT)) {
        // connect success
        OMS_DEBUG("Connect to server success after poll, server: {}", addr_str);
      } else {
        OMS_ERROR(
            "Failed to connect to server: {}, timeout: {}, error: {}", addr_str, (bool)(ret == 0), system_err(errno));
        close(sock);
        return OMS_CONNECT_FAILED;
      }
    } else {
      OMS_ERROR("Failed to connect to server: {}, error: {}", addr_str, system_err(errno));
      close(sock);
      return OMS_CONNECT_FAILED;
    }
  }

  sockfd = sock;
  return OMS_OK;
}

int connect(const std::string& server_path, bool block_mode, int timeout, int& sockfd)
{
  const int sock = socket(AF_LOCAL, SOCK_STREAM, 0);
  if (sock == -1) {
    OMS_ERROR("Failed to create socket, error: {}", logproxy::system_err(errno));
    return OMS_FAILED;
  }

  struct sockaddr_un server_sun {};
  memset(&server_sun, 0, sizeof(server_sun));
  server_sun.sun_family = AF_LOCAL;
  // server_sun.sun_path max length = 108
  strcpy(server_sun.sun_path, server_path.c_str());

  int ret = OMS_OK;
  if (!block_mode) {
    ret = set_non_block(sock);
    if (ret != 0) {
      close(sock);
      return OMS_FAILED;
    }
  }

  ret = connect(sock, (struct sockaddr*)&server_sun, sizeof(server_sun));
  if (ret != 0) {
    if (!block_mode && (errno == EINPROGRESS || errno == EAGAIN)) {
      struct pollfd pollfd {};
      pollfd.fd = sock;
      pollfd.events = POLLERR | POLLHUP | POLLOUT;
      pollfd.revents = 0;

      ret = poll(&pollfd, 1, timeout);
      if (ret == 1 && (pollfd.revents & POLLOUT)) {
        // connect success
        OMS_DEBUG("Connect to server success after poll, server: {}", server_path);
      } else {
        OMS_ERROR("Failed to connect to server: {}, timeout: {}, error: {}",
            server_path,
            (bool)(ret == 0),
            system_err(errno));
        close(sock);
        return OMS_CONNECT_FAILED;
      }
    } else {
      OMS_ERROR("Failed to connect to server: {}(server_sun.sun_path: {}), error: {}",
          server_path,
          server_sun.sun_path,
          system_err(errno));
      close(sock);
      return OMS_CONNECT_FAILED;
    }
  }

  sockfd = sock;
  return OMS_OK;
}

int connect(int sock, const sockaddr* serv_addr, const std::string& addr_str, bool block_mode, int timeout)
{
  return OMS_OK;
}

int listen(const char* host, int port, bool block_mode, bool reuse_address, int protocol)
{
  if (port < 0 || port > 65536) {
    OMS_STREAM_ERROR << "Invalid listen port: " << port;
    return OMS_FAILED;
  }

  struct in_addr bind_addr;
  bind_addr.s_addr = INADDR_ANY;
  if (host != nullptr) {
    struct hostent* hostent = gethostbyname(host);
    if (nullptr == hostent) {
      OMS_STREAM_ERROR << "gethostbyname return failed. address=" << host << ", error=" << strerror(errno);
      return OMS_FAILED;
    }
    bind_addr = *(struct in_addr*)hostent->h_addr;
  } else {
    host = "(not-set)";  // print log
  }

  struct sockaddr_in sockaddr;
  memset(&sockaddr, 0, sizeof(sockaddr));
  sockaddr.sin_family = PF_INET;
  sockaddr.sin_port = htons(port);
  sockaddr.sin_addr = bind_addr;

  int sock = socket(PF_INET, SOCK_STREAM, protocol);
  if (sock < 0) {
    OMS_STREAM_ERROR << "Failed to create socket. error=" << strerror(errno);
    return OMS_FAILED;
  }

  int ret = OMS_OK;
  if (reuse_address) {
    ret = set_reuse_addr(sock);
    if (ret != OMS_OK) {
      shutdown(sock, SHUT_RDWR);
      ::close(sock);
      return ret;
    }
  }

  ret = bind(sock, (struct sockaddr*)&sockaddr, sizeof(sockaddr));
  if (ret != OMS_OK) {
    OMS_STREAM_ERROR << "Failed to bind address '" << host << ":" << port << "', error: " << strerror(errno);
    shutdown(sock, SHUT_RDWR);
    ::close(sock);
    return OMS_FAILED;
  }

  ret = ::listen(sock, 10);
  if (ret != OMS_OK) {
    OMS_STREAM_ERROR << "Failed to listen on '" << host << ':' << port << "', error: " << strerror(errno);
    shutdown(sock, SHUT_RDWR);
    ::close(sock);
    return OMS_FAILED;
  }

  if (!block_mode) {
    ret = set_non_block(sock);
    if (ret != OMS_OK) {
      OMS_STREAM_ERROR << "Failed to set listen socket non-block mode, error: " << strerror(errno);
      shutdown(sock, SHUT_RDWR);
      ::close(sock);
      return OMS_FAILED;
    }
  }

  ret = set_close_on_exec(sock);
  if (ret != OMS_OK) {
    OMS_STREAM_ERROR << "Failed to set listen socket close on exec mode, error: " << strerror(errno);
    shutdown(sock, SHUT_RDWR);
    ::close(sock);
    return OMS_FAILED;
  }

  return sock;
}

int set_reuse_addr(int sock)
{
  int opt = 1;
  int ret = setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  if (ret != 0) {
    OMS_STREAM_ERROR << "Failed to set socket in 'REUSE_ADDR' mode. error=" << strerror(errno);
    return OMS_FAILED;
  }
  return OMS_OK;
}

int set_non_block(int fd)
{
  int flags = fcntl(fd, F_GETFL);
  if (flags == -1) {
    OMS_STREAM_WARN << "Failed to get flags of fd(" << fd << "). error=" << strerror(errno);
    return OMS_FAILED;
  }

  flags = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  if (flags == -1) {
    OMS_STREAM_WARN << "Failed to set non-block flags of fd(" << fd << "). error=" << strerror(errno);
    return OMS_FAILED;
  }
  return OMS_OK;
}

int set_close_on_exec(int fd)
{
  int flags = fcntl(fd, F_GETFL);
  if (flags == -1) {
    OMS_STREAM_WARN << "Failed to get flags of fd(" << fd << "). error=" << strerror(errno);
    return OMS_FAILED;
  }

  flags = fcntl(fd, F_SETFL, flags | O_CLOEXEC);
  if (flags == -1) {
    OMS_STREAM_WARN << "Failed to set close on exec flags of fd(" << fd << "). error=" << strerror(errno);
    return OMS_FAILED;
  }
  return OMS_OK;
}

bool port_in_use(int port)
{
  int sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  defer({
    shutdown(sockfd, SHUT_RDWR);
    close(sockfd);
  });
  if (sockfd == -1) {
    OMS_ERROR("Error creating socket:{}", strerror(errno));
    return true;
  }

  if (set_reuse_addr(sockfd) != OMS_OK) {
    return true;
  }
  struct sockaddr_in server_address {};
  memset(&server_address, 0, sizeof(server_address));
  server_address.sin_family = AF_INET;
  server_address.sin_addr.s_addr = htonl(INADDR_ANY);
  server_address.sin_port = htons(port);

  int result = bind(sockfd, (struct sockaddr*)&server_address, sizeof(server_address));
  if (result != OMS_OK) {
    OMS_ERROR("Failed to bind port:{},error:{}", port, strerror(errno));
  }
  return result != OMS_OK;
}

}  // namespace logproxy
}  // namespace oceanbase
