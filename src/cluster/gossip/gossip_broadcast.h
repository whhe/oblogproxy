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
#include "gossip.h"
#include "common/common.h"
#include "communication/io.h"
#include "communication/peer.h"
#include "communication/comm.h"
#include "cluster/node.h"
#include "common/log.h"
namespace oceanbase::logproxy {
class GossipProtocol;
class GossipTask : public Thread {
public:
  GossipTask(GossipProtocol& node, uint64_t gossip_interval_us, uint64_t gossip_nodes);

  void run() override;

private:
  uint64_t _gossip_interval_us;
  uint64_t _gossip_nodes;
  GossipProtocol& _node;
};

class PushPullTask : public Thread {
public:
  PushPullTask(GossipProtocol& node, uint64_t push_pull_interval_us, uint64_t gossip_nodes);

private:
  void run() override;
  uint64_t _push_pull_interval_us;
  // For push-pull action, the number of nodes is 1
  uint64_t _gossip_nodes = 1;
  GossipProtocol& _node;
};

class ProbeTask : public Thread {
public:
  ProbeTask(GossipProtocol& node, uint64_t probe_interval_us);

  void run() override;

  void reset_nodes();

  /*!
   * @brief Peer-to-peer node probing
   */
  void probe_to_node(Node* node);

protected:
  uint64_t _probe_interval_us;
  GossipProtocol& _node;
};

/************************************************************************/
bool validity_check(vector<Node>& nodes_selected, const Node& node_selected);

void random_nodes(vector<Node>& nodes_selected, GossipProtocol& node, uint64_t num);

int get_peer(const Node& node, Peer& peer) noexcept;

EventResult on_push_pull_msg(GossipProtocol& node, const Peer& peer, const Message& message);

int merge_state(GossipProtocol& node, const std::vector<PushNodeState>& remote_nodes);

// Update the liveness status of the current node
void online_node(GossipProtocol& node, PushNodeState const& online_node_state, bool bootstrap);

void suspect_node(GossipProtocol& node, PushNodeState const& suspect_node_state);

void offline_node(GossipProtocol& node, PushNodeState const& offline_node_state);

void correct(GossipProtocol& node, PushNodeState const& push_node_state, Node* node_state);

}  // namespace oceanbase::logproxy
