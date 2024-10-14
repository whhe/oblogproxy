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

#include "google/protobuf/message.h"

#include "log.h"
#include "common.h"
#include "message.h"
#include "msg_buf.h"
#include "encoder.h"
#include "logproxy.pb.h"
#include "gossip.pb.h"

namespace oceanbase {
namespace logproxy {
static const size_t PB_PACKET_HEADER_SIZE = PACKET_VERSION_SIZE + 1 /*type*/ + 4 /*packet size */;
static const size_t PB_PACKET_HEADER_SIZE_MAGIC = PACKET_MAGIC_SIZE + PB_PACKET_HEADER_SIZE;

int ProtobufEncoder::encode(const Message& msg, MsgBuf& buffer, size_t& raw_len)
{
  switch (msg.type()) {
    case MessageType::ERROR_RESPONSE: {
      return encode_error_response(msg, buffer);
    }
    case MessageType::HANDSHAKE_REQUEST_CLIENT: {
      return encode_client_handshake_request(msg, buffer);
    }
    case MessageType::HANDSHAKE_RESPONSE_CLIENT: {
      return encode_client_handshake_response(msg, buffer);
    }
    case MessageType::STATUS: {
      return encode_runtime_status(msg, buffer, raw_len);
    }
    case MessageType::DATA_CLIENT: {
      return encode_data_client(msg, buffer, raw_len);
    }
    case MessageType::GOSSIP_PING_MSG:
    case MessageType::GOSSIP_DATA_MSG:
    case MessageType::GOSSIP_ONLINE_MSG:
    case MessageType::GOSSIP_OFFLINE_MSG:
    case MessageType::GOSSIP_INDIRECT_PING_MSG:
    case MessageType::GOSSIP_PULL_PUSH_MSG:
    case MessageType::GOSSIP_SUSPECT_MSG:
    case MessageType::GOSSIP_PONG_MSG: {
      return encode_gossip_msg(msg, buffer, raw_len, msg.type());
    }
    default: {
      OMS_STREAM_ERROR << "Unknown message type: " << (int)msg.type();
      return OMS_FAILED;
    }
  }
}

static char* encode_message_header(MessageType type, int packet_size, bool magic)
{
  size_t header_len = magic ? PB_PACKET_HEADER_SIZE_MAGIC : PB_PACKET_HEADER_SIZE;
  char* buffer = (char*)malloc(header_len);
  if (nullptr == buffer) {
    OMS_STREAM_ERROR << "Failed to alloc memory for message _header. size=" << header_len;
    return nullptr;
  }

  int offset = 0;
  if (magic) {
    memcpy(buffer, PACKET_MAGIC, sizeof(PACKET_MAGIC));
    offset = sizeof(PACKET_MAGIC);
  }

  uint16_t version = cpu_to_be((uint16_t)MessageVersion::V2);
  memcpy(buffer + offset, &version, sizeof(version));
  offset += sizeof(version);

  int8_t message_type = (int8_t)type;
  memcpy(buffer + offset, &message_type, sizeof(message_type));
  offset += sizeof(message_type);

  uint32_t pb_packet_size = cpu_to_be((uint32_t)packet_size);
  memcpy(buffer + offset, &pb_packet_size, sizeof(pb_packet_size));
  return buffer;
}

int ProtobufEncoder::encode_message(
    const google::protobuf::Message& pb_msg, MessageType type, MsgBuf& buffer, bool magic)
{
  const size_t serialize_size = pb_msg.ByteSizeLong();
  // TODO max message size
  char* data_buffer = (char*)malloc(serialize_size);
  if (nullptr == data_buffer) {
    OMS_STREAM_ERROR << "Failed to alloc memory. size=" << serialize_size;
    return OMS_FAILED;
  }

  bool result = pb_msg.SerializeToArray(data_buffer, serialize_size);
  if (!result) {
    OMS_STREAM_ERROR << "Failed to serialize protobuf message. size=" << serialize_size;
    free(data_buffer);
    return OMS_FAILED;
  }
  //  Md5 md5(data_buffer, serialize_size);
  //  OMS_STREAM_INFO << "Serialized protobuf message, size: " << serialize_size << ", MD5: " << md5.done();

  char* header_buffer = encode_message_header(type, (int)serialize_size, magic);
  if (nullptr == header_buffer) {
    OMS_STREAM_ERROR << "Failed to encode client_hand_shake_request 's message _header";
    free(data_buffer);
    return OMS_FAILED;
  }

  buffer.push_back(header_buffer, magic ? PB_PACKET_HEADER_SIZE_MAGIC : PB_PACKET_HEADER_SIZE);
  buffer.push_back(data_buffer, serialize_size);
  return OMS_OK;
}

int ProtobufEncoder::encode_error_response(const Message& msg, MsgBuf& buffer)
{
  const ErrorMessage oms_msg = (const ErrorMessage&)msg;
  ErrorResponse pb_msg;
  pb_msg.set_code(oms_msg.code);
  pb_msg.set_message(oms_msg.message);
  return encode_message(pb_msg, oms_msg.type(), buffer, false);
}

int ProtobufEncoder::encode_client_handshake_request(const Message& msg, MsgBuf& buffer)
{
  const auto& request_message = (const ClientHandshakeRequestMessage&)msg;
  ClientHandshakeRequest pb_msg;
  pb_msg.set_log_type(request_message.log_type);
  pb_msg.set_ip(request_message.ip);
  pb_msg.set_id(request_message.id);
  pb_msg.set_version(request_message.version);
  pb_msg.set_enable_monitor(request_message.enable_monitor);
  pb_msg.set_configuration(request_message.configuration);
  return encode_message(pb_msg, request_message.type(), buffer, true);
}

int ProtobufEncoder::encode_client_handshake_response(const Message& msg, MsgBuf& buffer)
{
  const auto& response_message = (const ClientHandshakeResponseMessage&)msg;
  ClientHandshakeResponse pb_msg;
  pb_msg.set_code(response_message.code);
  pb_msg.set_ip(response_message.server_ip);
  pb_msg.set_version(response_message.server_version);
  return encode_message(pb_msg, response_message.type(), buffer, false);
}

int ProtobufEncoder::encode_runtime_status(const Message& msg, MsgBuf& buffer, size_t& raw_len)
{
  const RuntimeStatusMessage& runtime_status_message = (const RuntimeStatusMessage&)msg;
  RuntimeStatus pb_msg;
  pb_msg.set_ip(runtime_status_message.ip);
  pb_msg.set_port(runtime_status_message.port);
  pb_msg.set_stream_count(runtime_status_message.stream_count);
  pb_msg.set_worker_count(runtime_status_message.worker_count);
  int ret = encode_message(pb_msg, runtime_status_message.type(), buffer, false);
  raw_len = buffer.byte_size();
  return ret;
}

int ProtobufEncoder::encode_data_client(const Message& msg, MsgBuf& buffer, size_t& raw_len)
{
  RecordDataMessage& record_data_message = (RecordDataMessage&)msg;
  MsgBuf records_buffer;
  int ret = record_data_message.encode_log_records(records_buffer, raw_len);
  if (ret != OMS_OK) {
    OMS_STREAM_ERROR << "Failed to encode log records. ret=" << ret;
    return ret;
  }

  MsgBufReader records_buffer_reader(records_buffer);
  const size_t records_size = records_buffer_reader.byte_size();

  std::string pb_record_string(records_size, 0);
  // FIXME... big data packet copy here due to perf laging
  ret = records_buffer_reader.read((char*)pb_record_string.c_str(), records_size);
  if (ret != OMS_OK) {
    OMS_STREAM_ERROR << "Failed to read buffer from records buffer, size=" << records_size;
    return OMS_FAILED;
  }

  RecordData pb_msg;
  pb_msg.set_records(pb_record_string);
  pb_msg.set_compress_type((int)record_data_message.compress_type);
  pb_msg.set_raw_len(raw_len);
  pb_msg.set_compressed_len(records_size);
  pb_msg.set_count(record_data_message.count());
  return encode_message(pb_msg, record_data_message.type(), buffer, false);
}

int encode_gossip_ping_msg(const Message& msg, MsgBuf& buffer, size_t& raw_len)
{
  auto& ping_message = (GossipPingMessage&)msg;
  gossip::Ping pb_ping;
  pb_ping.set_ip(ping_message.ip);
  pb_ping.set_port(ping_message.port);
  pb_ping.set_seq_no(ping_message.seq_no);
  pb_ping.set_node(ping_message.node);
  pb_ping.set_source_node(ping_message.source_node);
  return ProtobufEncoder::encode_message(pb_ping, MessageType::GOSSIP_PING_MSG, buffer, false);
}

int encode_gossip_online_msg(const Message& msg, MsgBuf& buffer, size_t& raw_len)
{
  auto& meet_message = (GossipMeetMessage&)msg;
  gossip::Meet pb_meet;
  pb_meet.set_incarnation(meet_message.incarnation);
  pb_meet.set_meta(meet_message.meta);
  pb_meet.set_address(meet_message.ip);
  pb_meet.set_port(meet_message.port);
  pb_meet.set_node(meet_message.node);
  return ProtobufEncoder::encode_message(pb_meet, MessageType::GOSSIP_ONLINE_MSG, buffer, false);
}

int encode_gossip_offline_msg(const Message& msg, MsgBuf& buffer, size_t& raw_len)
{
  auto& offline_message = (GossipOfflineMessage&)msg;
  gossip::Offline pb_offline;
  pb_offline.set_incarnation(offline_message.incarnation);
  pb_offline.set_node(offline_message.node);
  pb_offline.set_offline_node(offline_message.offline_node);
  return ProtobufEncoder::encode_message(pb_offline, MessageType::GOSSIP_OFFLINE_MSG, buffer, false);
}

int encode_gossip_data_msg(const Message& msg, MsgBuf& buffer, size_t& raw_len)
{
  auto& data_message = (GossipDataMessage&)msg;
  switch (data_message.get_data_msg_type()) {
    case DataMessageType::METRIC: {
      auto& metric_data_msg = (MetricDataMessage&)msg;
      gossip::MetricDataMessage pb_metric_msg;
      for (const auto& metric : metric_data_msg.get_sys_metric()) {
        gossip::SysMetric pb_sys_metric;
        //        metric.serialize_to_pb(pb_sys_metric);
        metric.ip();
        pb_metric_msg.add_node_metric()->CopyFrom(pb_sys_metric);
      }
      return ProtobufEncoder::encode_message(pb_metric_msg, MessageType::GOSSIP_DATA_MSG, buffer, false);
    }
    default:
      OMS_ERROR("Unsupported data message type");
  }
  return OMS_FAILED;
}

int encode_gossip_suspect_msg(const Message& msg, MsgBuf& buffer, size_t& raw_len)
{
  auto& suspect_message = (GossipSuspectMessage&)msg;
  gossip::Suspect pb_suspect;
  pb_suspect.set_incarnation(suspect_message.incarnation);
  pb_suspect.set_node(suspect_message.node);
  pb_suspect.set_suspect_node(suspect_message.suspect_node);
  return ProtobufEncoder::encode_message(pb_suspect, MessageType::GOSSIP_SUSPECT_MSG, buffer, false);
}

int encode_gossip_indirect_ping_msg(const Message& msg, MsgBuf& buffer, size_t& raw_len)
{
  auto& indirect_ping_message = (GossipIndirectPingMessage&)msg;
  gossip::IndirectPing pb_indirect_ping;
  pb_indirect_ping.set_seq_no(indirect_ping_message.seq_no);
  pb_indirect_ping.set_node(indirect_ping_message.node);
  pb_indirect_ping.set_ip(indirect_ping_message.ip);
  pb_indirect_ping.set_port(indirect_ping_message.port);
  pb_indirect_ping.set_source_node(indirect_ping_message.source_node);
  pb_indirect_ping.set_source_address(indirect_ping_message.source_address);
  pb_indirect_ping.set_source_port(indirect_ping_message.source_port);
  return ProtobufEncoder::encode_message(pb_indirect_ping, MessageType::GOSSIP_INDIRECT_PING_MSG, buffer, false);
}

int encode_gossip_pull_push_msg(const Message& msg, MsgBuf& buffer, size_t& raw_len)
{
  auto& gossip_push_pull_message = (GossipPushPullMessage&)msg;
  gossip::PushPull pb_push_pull;
  pb_push_pull.set_nodes(gossip_push_pull_message.nodes);
  pb_push_pull.set_join(gossip_push_pull_message.join);
  for (const auto& item : gossip_push_pull_message.get_node_state()) {
    gossip::PushNodeState pb_push_node_status;
    pb_push_node_status.set_state(static_cast<gossip::State>(item.state));
    pb_push_node_status.set_port(item.port);
    pb_push_node_status.set_incarnation(item.incarnation);
    pb_push_node_status.set_address(item.address);
    pb_push_node_status.set_meta(item.meta);
    pb_push_pull.add_node_status()->CopyFrom(pb_push_node_status);
  }
  return ProtobufEncoder::encode_message(pb_push_pull, MessageType::GOSSIP_PULL_PUSH_MSG, buffer, false);
}

int encode_gossip_pong_msg(const Message& msg, MsgBuf& buffer, size_t& raw_len)
{
  auto& gossip_pong_message = (GossipPongMessage&)msg;
  gossip::Pong pb_pong;
  pb_pong.set_meta(gossip_pong_message.meta);
  pb_pong.set_seq_no(gossip_pong_message.seq_no);
  return ProtobufEncoder::encode_message(pb_pong, MessageType::GOSSIP_PONG_MSG, buffer, false);
}

int ProtobufEncoder::encode_gossip_msg(const Message& msg, MsgBuf& buffer, size_t& raw_len, MessageType msg_type)
{
  switch (msg_type) {
    case MessageType::GOSSIP_PING_MSG:
      return encode_gossip_ping_msg(msg, buffer, raw_len);
    case MessageType::GOSSIP_INDIRECT_PING_MSG:
      return encode_gossip_indirect_ping_msg(msg, buffer, raw_len);
    case MessageType::GOSSIP_PULL_PUSH_MSG:
      return encode_gossip_pull_push_msg(msg, buffer, raw_len);
    case MessageType::GOSSIP_SUSPECT_MSG:
      return encode_gossip_suspect_msg(msg, buffer, raw_len);
    case MessageType::GOSSIP_ONLINE_MSG:
      return encode_gossip_online_msg(msg, buffer, raw_len);
    case MessageType::GOSSIP_OFFLINE_MSG:
      return encode_gossip_offline_msg(msg, buffer, raw_len);
    case MessageType::GOSSIP_DATA_MSG:
      return encode_gossip_data_msg(msg, buffer, raw_len);
    case MessageType::GOSSIP_PONG_MSG:
      return encode_gossip_pong_msg(msg, buffer, raw_len);
    default:
      return OMS_FAILED;
  }
}

}  // namespace logproxy
}  // namespace oceanbase
