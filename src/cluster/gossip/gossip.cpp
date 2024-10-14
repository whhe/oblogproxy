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

#include "gossip.h"

#include <utility>

namespace oceanbase::logproxy {

int GossipProtocol::register_gossip_listener()
{
  //  _gossip_task.start();
  //  _push_pull_task.start();
  //  _probe_task.start();
  return OMS_OK;
}

GossipProtocol* GossipProtocol::create(TransportConfig transport_config)
{
  auto* node = new GossipProtocol();
  node->_transport = GossipTransport();
  // init transport
  //  TransportConfig transport_config{
  //      _s_config.cluster_communication_address.val(), _s_config.cluster_communication_port.val()};
  node->_transport.init(std::move(transport_config));
  node->register_gossip_listener();
  return node;
}

int GossipProtocol::join()
{
  return OMS_OK;
}

int GossipProtocol::stop()
{
  return OMS_OK;
}

std::shared_mutex& GossipProtocol::get_lock()
{
  return _lock;
}

const Meta& GossipProtocol::get_meta() const
{
  return _meta;
}

void GossipProtocol::set_meta(const Meta& meta)
{
  _meta = meta;
}

BlockingQueue<TransferMessage*>& GossipProtocol::get_msg_queue()
{
  return _msg_queue;
}

const GossipTransport& GossipProtocol::get_transport() const
{
  return _transport;
}

void GossipProtocol::set_transport(const GossipTransport& transport)
{
  _transport = transport;
}

const Node& GossipProtocol::get_node_info() const
{
  return _node_info;
}

void GossipProtocol::set_node_info(const Node& node_info)
{
  _node_info = node_info;
}

map<std::string, Node*>& GossipProtocol::get_status_map()
{
  return _status_map;
}

void GossipProtocol::set_status_map(const map<std::string, Node*>& status_map)
{
  _status_map = status_map;
}

map<std::string, Suspicion*>& GossipProtocol::get_suspicion_map()
{
  return _suspicion_map;
}

void GossipProtocol::set_suspicion_map(const map<std::string, Suspicion*>& suspicion_map)
{
  _suspicion_map = suspicion_map;
}

uint64_t GossipProtocol::get_next_incarnation()
{
  return OMS_ATOMIC_INC(this->incarnation);
}

uint64_t& GossipProtocol::get_incarnation()
{
  return incarnation;
}

void GossipProtocol::set_incarnation(uint64_t const& incarnation)
{
  this->incarnation = incarnation;
}

int GossipProtocol::drain_to_queue(TransferMessage* msg)
{
  while (!_msg_queue.offer(msg, _s_config.send_timeout_us.val())) {
    OMS_INFO("reader transfer queue full({}), retry...");
  }
  return OMS_OK;
}

int GossipProtocol::drain_to_queue(Message* msg)
{
  auto* transfer_message = new TransferMessage(msg);
  while (!_msg_queue.offer(transfer_message, _s_config.send_timeout_us.val())) {
    OMS_INFO("reader transfer queue full({}), retry...");
  }
  return OMS_OK;
}

uint64_t& GossipProtocol::get_probe_index()
{
  return probe_index;
}

void GossipProtocol::set_probe_index(uint64_t probe_index)
{
  GossipProtocol::probe_index = probe_index;
}

uint64_t GossipProtocol::get_gossip_interval_us() const
{
  return gossip_interval_us;
}

void GossipProtocol::set_gossip_interval_us(uint64_t interval_us)
{
  GossipProtocol::gossip_interval_us = interval_us;
}

uint64_t GossipProtocol::get_gossip_nodes() const
{
  return gossip_nodes;
}

void GossipProtocol::set_gossip_nodes(uint64_t nodes)
{
  GossipProtocol::gossip_nodes = nodes;
}

uint64_t GossipProtocol::get_gossip_determine_interval_us() const
{
  return gossip_determine_interval_us;
}

void GossipProtocol::set_gossip_determine_interval_us(uint64_t determine_interval_us)
{
  GossipProtocol::gossip_determine_interval_us = determine_interval_us;
}

uint64_t GossipProtocol::get_probe_interval_us() const
{
  return probe_interval_us;
}

void GossipProtocol::set_probe_interval_us(uint64_t interval_us)
{
  GossipProtocol::probe_interval_us = interval_us;
}

uint64_t GossipProtocol::get_probe_timeout_us() const
{
  return probe_timeout_us;
}

void GossipProtocol::set_probe_timeout_us(uint64_t timeout_us)
{
  GossipProtocol::probe_timeout_us = timeout_us;
}

uint64_t GossipProtocol::get_push_pull_interval_us() const
{
  return push_pull_interval_us;
}

void GossipProtocol::set_push_pull_interval_us(uint64_t interval_us)
{
  GossipProtocol::push_pull_interval_us = interval_us;
}

uint64_t GossipProtocol::get_node_num() const
{
  return this->_nodes.size();
}

vector<Node*>& GossipProtocol::get_nodes()
{
  return _nodes;
}

uint64_t& GossipProtocol::get_seq_no()
{
  return _seq_no;
}

uint64_t GossipProtocol::get_next_seq_no()
{
  return OMS_ATOMIC_INC(_seq_no);
}

TransferMessage::TransferMessage(Message* msg)
{}

TransferMessage::~TransferMessage()
{
  if (_msg != nullptr) {
    delete (_msg);
    _msg = nullptr;
  }
}

}  // namespace oceanbase::logproxy
