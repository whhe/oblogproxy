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

#include <vector>
#include <unistd.h>

#include "mysql_protocol.h"
#include "log.h"
#include "common.h"
#include "communication/io.h"
#include "msg_buf.h"

namespace oceanbase::logproxy {

MysqlProtocol::~MysqlProtocol()
{
  close();
}

void MysqlProtocol::close()
{
  if (_sockfd >= 0) {
    ::close(_sockfd);
    _sockfd = -1;
  }
  _server_addr = "";
  _database = "";
  _username = "";
  _passwd_sha1 = "";
}

int MysqlProtocol::login(const std::string& host, uint32_t port, const std::string& username,
    const std::string& passwd_sha1, const std::string& database)
{
  _server_addr = std::string(host) + std::string(":") + std::to_string(port);
  _username = username;
  _passwd_sha1 = passwd_sha1;
  _database = database;

  int ret = connect(host.c_str(), port, false, _detect_timeout, _sockfd);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to connect to server: {}, user: {}", _server_addr, _username);
    return OMS_FAILED;
  }

  OMS_INFO("Connect to server success: {}, user: {}", _server_addr, _username);
  return handshake();
}

int MysqlProtocol::login(const std::string& socket_path, const std::string& username)
{
  _server_addr = socket_path;
  _username = username;

  int ret = connect(socket_path, false, _detect_timeout, _sockfd);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to connect to server: {}", socket_path);
    return OMS_FAILED;
  }

  OMS_INFO("Connect to server through unix domain socket success: {}, user: {}", _server_addr, _username);
  return handshake();
}

int MysqlProtocol::handshake()
{
  // https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
  // 1 receive initial handshake
  MsgBuf msgbuf;
  uint8_t sequence = 0;
  uint32_t packet_length = 0;
  int ret = recv_mysql_packet(_sockfd, _detect_timeout, packet_length, sequence, msgbuf);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to receive handshake packet from: {}, error: {}", _server_addr, system_err(errno));
    return ret;
  }
  OMS_DEBUG("Receive handshake packet from server: {}, user: {}", _server_addr, _username);

  MySQLInitialHandShakePacket handshake_packet;
  ret = handshake_packet.decode(msgbuf);
  if (ret != OMS_OK || !handshake_packet.scramble_valid()) {
    OMS_ERROR("Failed to decode_payload initial handshake packet or does not has a valid scramble: {}, length={}, "
              "server: {}",
        handshake_packet.scramble_valid(),
        packet_length,
        _server_addr);
    return ret;
  }

  const std::vector<char>& scramble = handshake_packet.scramble();

  // 2 calculate the password combined with scramble buffer -> auth information
  std::vector<char> auth;
  if (!_passwd_sha1.empty()) {
    ret = calc_mysql_auth_info(scramble, auth);
    if (ret != OMS_OK) {
      OMS_ERROR("Failed to calc login info from: {}, user: {}", _server_addr, _username);
      return ret;
    }
  }

  // 3 send the handshake response with auth information
  ret = send_auth(auth, _database, sequence + 1);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to send handshake response message to [{}], user={}", _username);
    return OMS_FAILED;
  }

  // 4 receive response from server
  msgbuf.reset();
  ret = recv_mysql_packet(_sockfd, _detect_timeout, packet_length, sequence, msgbuf);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to recv handshake auth response from: {}, user: {}", _server_addr, _username);
    return OMS_FAILED;
  }

  MySQLOkPacket ok_packet;
  ret = ok_packet.decode(msgbuf);
  if (ret == OMS_OK) {
    OMS_INFO("Auth user success of server: {}, user: {}", _server_addr, _username);
  } else {
    MySQLErrorPacket error_packet;
    error_packet.decode(msgbuf);
    OMS_ERROR("Auth user failed of server: {},user:{},reason:{}", _server_addr, _username, error_packet._message);
  }
  return ret;
}

int MysqlProtocol::calc_mysql_auth_info(const std::vector<char>& scramble, std::vector<char>& auth)
{
  // SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
  // SHA1(password) -> stage1
  // SHA1(SHA1(password)) -> stage2
  SHA1 sha1;
  SHA1::ResultCode result_code = sha1.input((const unsigned char*)_passwd_sha1.data(), _passwd_sha1.size());
  if (result_code != SHA1::SHA_SUCCESS) {
    OMS_STREAM_ERROR << "Failed to calc sha1(step input) of passwd stage1 to stage2. result code=" << result_code;
    return OMS_FAILED;
  }

  std::vector<char> passwd_stage2;
  passwd_stage2.resize(SHA1::SHA1_HASH_SIZE);
  result_code = sha1.get_result((unsigned char*)passwd_stage2.data());
  if (result_code != SHA1::SHA_SUCCESS) {
    OMS_STREAM_ERROR << "Failed to calc sha1(step get_result) of passwd stage1 to stage2. result code=" << result_code;
    return OMS_FAILED;
  }

  std::vector<char> scramble_combined;
  scramble_combined.reserve(scramble.size() + passwd_stage2.size());
  scramble_combined.assign(scramble.begin(), scramble.end());
  scramble_combined.insert(scramble_combined.end(), passwd_stage2.begin(), passwd_stage2.end());

  sha1.reset();
  result_code = sha1.input((const unsigned char*)scramble_combined.data(), scramble_combined.size());
  if (result_code != SHA1::SHA_SUCCESS) {
    OMS_STREAM_ERROR << "Failed to calc sha1(step input) of combined. result code=" << result_code;
    return OMS_FAILED;
  }

  std::vector<char> sha_combined;
  sha_combined.resize(SHA1::SHA1_HASH_SIZE);
  result_code = sha1.get_result((unsigned char*)sha_combined.data());
  if (result_code != SHA1::SHA_SUCCESS) {
    OMS_STREAM_ERROR << "Failed to calc sha1(step get_result) of combined. result code=" << result_code;
    return OMS_FAILED;
  }

  if (_passwd_sha1.size() != sha_combined.size()) {
    OMS_STREAM_ERROR << "the length of password stage1(sha1)(" << _passwd_sha1.size()
                     << ") != the length of sha_combined(" << sha_combined.size() << ")";
    return OMS_FAILED;
  }

  auth.resize(sha_combined.size());
  my_xor((const unsigned char*)_passwd_sha1.data(),
      (const unsigned char*)sha_combined.data(),
      auth.size(),
      (unsigned char*)auth.data());
  return OMS_OK;
}

int MysqlProtocol::send_auth(const std::vector<char>& auth_info, const std::string& database, uint8_t sequence)
{
  MySQLHandShakeResponsePacket handshake_response_packet(_username, database, auth_info);
  MsgBuf msgbuf;
  int ret = handshake_response_packet.encode(msgbuf);
  if (ret != OMS_OK) {
    OMS_STREAM_ERROR << "Failed to encode handshake response packet";
    return -1;
  }

  ret = send_mysql_packet(_sockfd, msgbuf, sequence);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to send handshake response to server: {}", _server_addr);
    return ret;
  }
  return OMS_OK;
}

int MysqlProtocol::query(const std::string& sql, MySQLResultSet& rs)
{
  OMS_DEBUG("Query obmysql SQL:{}", sql);
  rs.reset();

  MsgBuf msgbuf;
  MySQLQueryPacket packet(sql);
  int ret = packet.encode_inplace(msgbuf);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to encode observer sql packet, ret:{}", ret);
    return ret;
  }
  ret = send_mysql_packet(_sockfd, msgbuf, 0);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to send query packet to server: {}", _server_addr);
    return OMS_CONNECT_FAILED;
  }

  ret = recv_mysql_packet(_sockfd, _detect_timeout, msgbuf);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to recv query packet from server: {}", _server_addr);
    return OMS_CONNECT_FAILED;
  }

  MySQLQueryResponsePacket query_resp;
  ret = query_resp.decode(msgbuf);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to query observer:{}", query_resp._err._message);
    rs.code = query_resp._err._code;
    rs.message = query_resp._err._message;
    return OMS_FAILED;
  }

  if (query_resp.col_count() == 0) {
    // Ok packet
    rs.affect_rows = query_resp._ok.get_affected_rows();
    return OMS_OK;
  }

  // column definitions
  rs.col_count = query_resp.col_count();
  rs.cols.resize(query_resp.col_count());
  for (uint64_t i = 0; i < query_resp.col_count(); ++i) {
    ret = recv_mysql_packet(_sockfd, _detect_timeout, msgbuf);
    if (ret != OMS_OK) {
      OMS_ERROR("Failed to recv column defines packet from server: {}", _server_addr);
      return OMS_CONNECT_FAILED;
    }

    MySQLCol column;
    column.decode(msgbuf);
    rs.cols[i] = column;
  }

  // eof
  ret = recv_mysql_packet(_sockfd, _detect_timeout, msgbuf);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to recv eof packet from server: {}", _server_addr);
    return OMS_CONNECT_FAILED;
  }
  MySQLEofPacket eof;
  ret = eof.decode(msgbuf);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to decode eof packet from server: {}", _server_addr);
    return OMS_OK;
  }

  // rows
  while (true) {
    ret = recv_mysql_packet(_sockfd, _detect_timeout, msgbuf);
    if (ret != OMS_OK || msgbuf.begin()->buffer() == nullptr) {
      OMS_ERROR("Failed to recv row packet from server: {}", _server_addr);
      return OMS_CONNECT_FAILED;
    }
    uint8_t eof_code = msgbuf.begin()->buffer()[0];
    if (eof_code == 0xfe) {
      break;
    }

    MySQLRow row(query_resp.col_count());
    row.decode(msgbuf);
    rs.rows.emplace_back(row);
  }

  return OMS_OK;
}

int MysqlProtocol::route_query(MsgBuf& request_buf, MsgBuf& resp_buf)
{
  int ret = send_mysql_packet(_sockfd, request_buf, 0);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to send query packet to server: {}", _server_addr);
    return OMS_CONNECT_FAILED;
  }

  MsgBuf msg_buf;

  // 1. resp packet
  ret = recv_mysql_packet(_sockfd, _detect_timeout, msg_buf);
  if (ret != OMS_OK) {
    OMS_ERROR("Failed to recv query packet from server: {}", _server_addr);
    return OMS_CONNECT_FAILED;
  }
  resp_buf.push_back_copy(msg_buf.begin()->buffer(), msg_buf.begin()->size());

  uint8_t packet_ret = msg_buf.begin()->buffer()[0];
  if (packet_ret == 0xff || packet_ret == 0xfb || packet_ret == 0x00) {
    OMS_INFO("Returned with the end of first query response, packet_code: {}", packet_ret);
    return OMS_OK;
  }

  // 2. meta packet
  while (true) {
    ret = recv_mysql_packet(_sockfd, _detect_timeout, msg_buf);
    if (ret != OMS_OK || msg_buf.begin()->buffer() == nullptr) {
      OMS_ERROR("Failed to recv row packet from server: {}", _server_addr);
      return OMS_CONNECT_FAILED;
    }
    resp_buf.push_back_copy(msg_buf.begin()->buffer(), msg_buf.begin()->size());

    uint8_t packet_code = msg_buf.begin()->buffer()[0];
    // eof
    if (packet_code == 0xfe) {
      break;
    }
  }

  // 3. row packet
  uint8_t packet_code;
  while (true) {
    ret = recv_mysql_packet(_sockfd, _detect_timeout, msg_buf);
    if (ret != OMS_OK || msg_buf.begin()->buffer() == nullptr) {
      OMS_ERROR("Failed to recv row packet from server: {}", _server_addr);
      return OMS_CONNECT_FAILED;
    }
    resp_buf.push_back_copy(msg_buf.begin()->buffer(), msg_buf.begin()->size());

    packet_code = msg_buf.begin()->buffer()[0];
    // eof or err
    if (packet_code == 0xfe || packet_code == 0xff) {
      break;
    }
  }

  OMS_INFO("Returned with the end of rows packet, packet_code: {}", packet_code);
  return OMS_OK;
}

}  // namespace oceanbase::logproxy
