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

#include "gossip_broadcast.h"
#include "timer.h"
#include "msg_buf.h"
#include "communication/comm.h"
#include <algorithm>
#include <cmath>

namespace oceanbase::logproxy {

void GossipTask::run()
{
  Timer timer;
  Comm comm;
  comm.init();
  while (is_run()) {
    std::vector<Node> nodes_selected;
    random_nodes(nodes_selected, this->_node, this->_gossip_nodes);

    // Send messages in the queue
    std::vector<TransferMessage*> transfer_message;
    if (_node.get_msg_queue().poll(transfer_message, _s_config.read_timeout_us.val()) && !transfer_message.empty()) {
      for (const Node& node : nodes_selected) {
        Peer peer;
        if (get_peer(node, peer) != OMS_OK) {
          OMS_ERROR("Failed to get peer to node: {}", node.ip());
          continue;
        }

        for (auto* msg : transfer_message) {
          if (comm.send_message(peer, *(msg->_msg), true) != OMS_OK) {
            OMS_ERROR("Failed to write gossip to node: {}", node.ip());
            break;
          }
        }
      }
    };
    timer.sleep(_gossip_interval_us);
  }
}

GossipTask::GossipTask(GossipProtocol& node, uint64_t gossip_interval_us, uint64_t gossip_nodes)
    : _gossip_interval_us(gossip_interval_us), _gossip_nodes(gossip_nodes), _node(node)
{}

PushPullTask::PushPullTask(GossipProtocol& node, uint64_t push_pull_interval_us, uint64_t gossip_nodes)
    : _node(node), _push_pull_interval_us(push_pull_interval_us), _gossip_nodes(gossip_nodes)
{}

void PushPullTask::run()
{
  Comm comm;
  comm.init();
  //  comm.set_read_callback([this](const Peer& peer, const Message& msg) { return on_push_pull_msg(0, peer, msg); });
  while (is_run()) {
    // Send the status information of this node to a randomly selected node
    std::vector<Node> nodes_selected;
    random_nodes(nodes_selected, this->_node, this->_gossip_nodes);
    if (nodes_selected.empty()) {
      continue;
    }
    Peer peer;
    get_peer(nodes_selected.at(0), peer);
    GossipPushPullMessage gossip_push_pull_message;
    if (comm.send_message(peer, gossip_push_pull_message, true) != OMS_OK) {
      OMS_INFO("Failed to write push-pull to node: {}", nodes_selected.at(0).id());
      continue;
    }
    // Get the return value from the send request and merge the status information，read callback
  }
}

ProbeTask::ProbeTask(GossipProtocol& node, uint64_t probe_interval_us)
    : _node(node), _probe_interval_us(probe_interval_us)
{}

void ProbeTask::run()
{
  uint64_t probe_num = 0;
START:
  this->_node.get_lock().lock_shared();
  if (probe_num >= this->_node.get_node_num()) {
    this->_node.get_lock().unlock();
    return;
  }

  if (this->_node.get_probe_index() >= this->_node.get_node_num()) {
    this->_node.get_lock().unlock_shared();
    reset_nodes();
    this->_node.set_probe_index(0);
    probe_num++;
    goto START;
  }

  this->_node.get_lock().unlock_shared();
  uint64_t probe_index = _node.get_probe_index();
  this->_node.get_probe_index()++;
  if (this->_node.get_nodes()[probe_index]->id() == this->_node.get_node_info().id() ||
      this->_node.get_nodes()[probe_index]->state() == OFFLINE) {
    probe_num++;
    goto START;
  }
}

void ProbeTask::reset_nodes()
{
  lock_guard<shared_mutex> lock_guard(this->_node.get_lock());
  std::uint64_t offline_num = 0;
  std::uint64_t node_num = this->_node.get_node_num();
  for (int i = 0; i < node_num - offline_num; ++i) {
    if (this->_node.get_nodes()[i]->state() != OFFLINE) {
      continue;
    }

    /*!
     * @brief It is not yet time to judge offline, ignore
     */
    if (Timer::now() - this->_node.get_nodes()[i]->last_modify() <= this->_node.get_gossip_determine_interval_us()) {
      continue;
    }

    std::swap(this->_node.get_nodes()[i], this->_node.get_nodes()[node_num - offline_num - 1]);
    offline_num++;
    i--;
  }
  // std::uint64_t offline_index = node_num - offline_num;

  for (int index = 0; index < this->_node.get_node_num(); ++index) {
    delete (this->_node.get_status_map().find(this->_node.get_nodes()[index]->id())->second);
    this->_node.get_status_map().erase(this->_node.get_nodes()[index]->id());
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::shuffle(this->_node.get_nodes().begin(), this->_node.get_nodes().end(), gen);
}

void ProbeTask::probe_to_node(Node* node)
{
  OMS_INFO("Start peer-to-peer biopsy");
  GossipPingMessage gossip_ping_message;
  gossip_ping_message.node = node->id();
  gossip_ping_message.seq_no = this->_node.get_next_seq_no();
  gossip_ping_message.ip = this->_node.get_node_info().ip();
  gossip_ping_message.port = this->_node.get_node_info().port();
  gossip_ping_message.source_node = this->_node.get_node_info().id();
}

/************************************************************/

int get_peer(const Node& node, Peer& peer) noexcept
{
  int sockfd = 0;
  int ret = connect(node.ip().c_str(), node.port(), true, 0, sockfd);
  if (ret != OMS_OK || sockfd <= 0) {
    OMS_ERROR("Failed to connect {}:{}", node.ip(), node.port());
    return OMS_FAILED;
  }

  set_non_block(sockfd);
  OMS_INFO("Connected to {}:{}  with sockfd: ", node.ip(), node.port(), sockfd);
  struct sockaddr_in peer_addr;
  socklen_t len;
  ret = getpeername(sockfd, (struct sockaddr*)&peer_addr, &len);
  if (ret == 0 && peer_addr.sin_addr.s_addr != 0) {
    Peer p(peer_addr.sin_addr.s_addr, ntohs(peer_addr.sin_port), sockfd);
    OMS_INFO("fetched peer: ", p.to_string());
  } else {
    OMS_WARN("Failed to fetch peer info of fd:{}, errno:{}, error:{}", sockfd, errno, strerror(errno));
  }
  peer.clone(Peer(peer_addr.sin_addr.s_addr, htons(peer_addr.sin_port), sockfd));
  return OMS_OK;
}

void random_nodes(vector<Node>& nodes_selected, GossipProtocol& node, uint64_t num)
{
  lock_guard<shared_mutex> lock_guard(node.get_lock());
  // Randomly select _gossip_nodes nodes.At most 2 n selections or _gossip nodes have been selected, then the
  // selection operation
  vector<Node> nodes;
  for (const auto& it : node.get_status_map()) {
    nodes.push_back(*it.second);
  }

  for (int i = 0, j = 0; i < 2 * nodes.size() && j < num; ++i) {
    uint64_t node_index = random_number(num - 1);

    if (node_index >= nodes.size()) {
      continue;
    }

    Node node_selected = nodes.at(node_index);
    if (validity_check(nodes_selected, node_selected) && node.get_node_info().id() != node_selected.id()) {
      nodes_selected.emplace_back(node_selected);
    }
  }
}

bool validity_check(vector<Node>& nodes_selected, const Node& node_selected)
{
  // Determine whether the node has been selected
  for (const auto& node : nodes_selected) {
    if (node.id() == node_selected.id()) {
      return false;
    }
  }

  return node_selected.state() != OFFLINE;
}

EventResult on_push_pull_msg(GossipProtocol& node, const Peer& peer, const Message& message)
{
  switch (message.type()) {
    case MessageType::GOSSIP_PULL_PUSH_MSG: {
      const auto& msg = (const GossipPushPullMessage&)message;
      // merge node status
      merge_state(node, msg.get_node_state());
      break;
    }
    default:
      return EventResult::ER_CLOSE_CHANNEL;
  }
  return EventResult::ER_SUCCESS;
}

int merge_state(GossipProtocol& node, const std::vector<PushNodeState>& remote_nodes)
{
  for (const auto& push_node_state : remote_nodes) {
    switch (push_node_state.state) {
      case gossip::online: {
        online_node(node, push_node_state, false);
        break;
      }
      case gossip::offline: {
        offline_node(node, push_node_state);
        break;
      }
      case gossip::suspect: {
        suspect_node(node, push_node_state);
        break;
      }
      case gossip::State_INT_MIN_SENTINEL_DO_NOT_USE_:
        break;
      case gossip::State_INT_MAX_SENTINEL_DO_NOT_USE_:
        break;
    }
  }
  return OMS_OK;
}

void online_node(GossipProtocol& node, PushNodeState const& online_node_state, bool bootstrap)
{
  bool is_update = false;
  Node* node_status;
  map<string, Node*> status_map = node.get_status_map();
  map<string, Suspicion*> suspicion_map = node.get_suspicion_map();
  if (status_map.find(online_node_state.name) == status_map.end()) {
    node_status = new Node();
    node_status->set_id(online_node_state.name);
    node_status->set_port(online_node_state.port);
    node_status->set_ip(online_node_state.address);

    node_status->set_state(ONLINE);
    status_map[online_node_state.name] = node_status;
  } else {
    node_status = status_map.find(online_node_state.name)->second;
    if (node_status->ip() != online_node_state.address || node_status->port() != online_node_state.port) {
      if (node_status->state() == OFFLINE) {
        // Update node ip port information
        OMS_INFO("Need to update node ip port information");
        is_update = true;
      }
    }
  }

  bool is_local_node = node_status->id() == node.get_node_info().id();
  bool is_old_incarnation = online_node_state.incarnation <= node_status->incarnation();

  if (is_old_incarnation && !is_local_node && !is_update) {
    return;
  }

  if (is_old_incarnation && is_local_node) {
    return;
  }

  // If the node exists in the node list that we suspect may be offline, we need to clear its records and terminate the
  // process of becoming an offline node
  if (suspicion_map.find(node_status->id()) != suspicion_map.end()) {
    suspicion_map.erase(node_status->id());
  }

  //  auto old_state = node_status->state;
  //  auto old_meta = node_status->metric();

  // It is not an initialization action of this node, which means that some nodes think that we have just joined the
  // cluster at this time, and we need to update this information to the cluster at this time. Used to correct this
  // error state and increment the node's version number at the same time.
  if (!bootstrap && is_local_node) {
    if (online_node_state.incarnation == node_status->incarnation()) {

      return;
    }
    // Correct the message for this node
    correct(node, online_node_state, node_status);

  } else {
    auto* meet_message = new GossipMeetMessage();
    meet_message->node = online_node_state.name;
    meet_message->ip = online_node_state.address;
    meet_message->port = online_node_state.port;
    // broadcast the message to other nodes
    node.drain_to_queue(meet_message);
  }
}

void offline_node(GossipProtocol& node, PushNodeState const& offline_node_state)
{
  OMS_INFO("Try to offline node {}", node.get_node_info().id());
  lock_guard<shared_mutex> lock_guard(node.get_lock());

  Node* node_status;
  map<string, Node*> status_map = node.get_status_map();
  map<string, Suspicion*> suspicion_map = node.get_suspicion_map();

  if (status_map.find(offline_node_state.name) == status_map.end()) {
    return;
  }

  node_status = status_map.find(offline_node_state.name)->second;

  if (offline_node_state.incarnation < node_status->incarnation()) {
    return;
  }

  // Assuming that the node is currently in the process of going offline, there is no need to do anything
  if (node_status->state() == OFFLINE) {
    return;
  }

  // Clean up the suspicious count of this node for this node
  if (suspicion_map.find(node_status->id()) != suspicion_map.end()) {
    suspicion_map.erase(node_status->id());
  }

  if (node_status->id() == node.get_node_info().id() && node_status->state() == SUSPECT) {
    // Correct the message for this node
    correct(node, offline_node_state, node_status);
  }

  auto* offline_msg = new GossipOfflineMessage();
  offline_msg->node = offline_node_state.name;
  offline_msg->offline_node = offline_node_state.name;
  offline_msg->incarnation = offline_node_state.incarnation;
  // broadcast the message to other nodes
  node.drain_to_queue(offline_msg);

  node_status->set_incarnation(offline_node_state.incarnation);

  node_status->set_last_modify(Timer::now());
}

void suspect_node(GossipProtocol& node, PushNodeState const& suspect_node_state)
{
  OMS_INFO("Try to suspect node {}", node.get_node_info().id());
  lock_guard<shared_mutex> lock_guard(node.get_lock());
  Node* node_status;
  map<string, Node*> status_map = node.get_status_map();
  map<string, Suspicion*> suspicion_map = node.get_suspicion_map();

  if (status_map.find(suspect_node_state.name) == status_map.end()) {
    return;
  }

  node_status = status_map.find(suspect_node_state.name)->second;

  if (suspect_node_state.incarnation < node_status->incarnation()) {
    return;
  }

  // At this point we know the existence of the node but no suspicion information is recorded for it
  if (suspicion_map.find(suspect_node_state.name) == suspicion_map.end()) {
    auto* suspicion = new Suspicion();
    suspicion->suspicious_vote_count++;
    suspicion->suspect_record.emplace(suspect_node_state.name, true);
    suspicion_map.emplace(suspect_node_state.name, suspicion);

    auto* suspect_message = new GossipSuspectMessage();
    suspect_message->node = suspect_node_state.name;
    suspect_message->suspect_node = node.get_node_info().id();
    suspect_message->incarnation = suspect_node_state.incarnation;
    // broadcast the message to other nodes
    node.drain_to_queue(suspect_message);
    return;
  }

  if (node_status->state() != ONLINE) {
    return;
  }

  if (suspect_node_state.name == node.get_node_info().id()) {
    // Correct the message for this node
    correct(node, suspect_node_state, node_status);
    return;
  }

  auto* suspect_message = new GossipSuspectMessage();
  suspect_message->node = suspect_node_state.name;
  suspect_message->suspect_node = node.get_node_info().id();
  suspect_message->incarnation = suspect_node_state.incarnation;
  // broadcast the message to other nodes
  node.drain_to_queue(suspect_message);

  node_status->set_incarnation(suspect_node_state.incarnation);
  node_status->set_state(SUSPECT);
  node_status->set_last_modify(Timer::now());
}

void correct(GossipProtocol& node, PushNodeState const& push_node_state, Node* node_state)
{
  OMS_INFO("Correct the state information of the node {} to the cluster", node.get_node_info().id());
  lock_guard<shared_mutex> lock_guard(node.get_lock());

  auto inc = node.get_incarnation();
  inc = inc > push_node_state.incarnation ? inc : push_node_state.incarnation;

  inc = OMS_ATOMIC_INC(inc);
  auto* meet_message = new GossipMeetMessage();
  meet_message->node = node_state->id();
  meet_message->ip = node_state->ip();
  meet_message->port = node_state->port();
  // TODO state

  node.drain_to_queue(meet_message);
}

}  // namespace oceanbase::logproxy