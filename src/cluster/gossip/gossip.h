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
#include <string>
#include <map>
#include <stdint-gcc.h>
#include <shared_mutex>
#include "blocking_queue.hpp"
#include "metric/sys_metric.h"
#include "gossip.pb.h"
#include "config.h"
#include "cluster/transport.h"
#include "codec/message.h"
#include "cluster/node.h"
#include "cluster/gossip/gossip_broadcast.h"
namespace oceanbase::logproxy {

static Config& _s_config = Config::instance();

// Suspicious information about node
struct Suspicion {
  // Confirm suspicious vote count
  uint64_t suspicious_vote_count;
  // It is used to record the nodes that suspect that the node is offline
  std::map<std::string, bool> suspect_record;
};

struct TransferMessage {
  TransferMessage(Message* msg);
  ~TransferMessage();
  Message* _msg;
  // Transmission retries
  uint64_t _retransmit;
};

class GossipProtocol {
public:
  GossipProtocol()
  {}
  static GossipProtocol* create(TransportConfig transport_config);
  static int join();
  static int stop();

public:
  ~GossipProtocol() = default;
  /*
   * @params
   * @returns Registration result 0 means the registration is successful, other means the registration failed
   * @description Register gossip protocol listening service
   * @date 2023/1/9 16:07
   */
  int register_gossip_listener();

  std::shared_mutex& get_lock();

  const Meta& get_meta() const;

  void set_meta(const Meta& meta);

  BlockingQueue<TransferMessage*>& get_msg_queue();

  const GossipTransport& get_transport() const;

  void set_transport(const GossipTransport& transport);

  const Node& get_node_info() const;

  void set_node_info(const Node& node_info);

  map<std::string, Node*>& get_status_map();

  void set_status_map(const map<std::string, Node*>& status_map);

  map<std::string, Suspicion*>& get_suspicion_map();

  void set_suspicion_map(const map<std::string, Suspicion*>& suspicion_map);

  uint64_t get_next_incarnation();

  uint64_t& get_incarnation();

  void set_incarnation(uint64_t const& incarnation);

  int drain_to_queue(TransferMessage* msg);

  int drain_to_queue(Message* msg);

  uint64_t& get_probe_index();

  void set_probe_index(uint64_t probe_index);

  uint64_t get_gossip_interval_us() const;

  void set_gossip_interval_us(uint64_t interval_us);

  uint64_t get_gossip_nodes() const;

  void set_gossip_nodes(uint64_t nodes);

  uint64_t get_gossip_determine_interval_us() const;

  void set_gossip_determine_interval_us(uint64_t determine_interval_us);

  uint64_t get_probe_interval_us() const;

  void set_probe_interval_us(uint64_t interval_us);

  uint64_t get_probe_timeout_us() const;

  void set_probe_timeout_us(uint64_t timeout_us);

  uint64_t get_push_pull_interval_us() const;

  void set_push_pull_interval_us(uint64_t interval_us);

  uint64_t get_node_num() const;

  vector<Node*>& get_nodes();

  uint64_t& get_seq_no();

  uint64_t get_next_seq_no();

private:
  //  std::string _name;
  //  std::string _ip;
  //  uint32_t _port;

  uint64_t incarnation;
  uint64_t _seq_no;
  Node _node_info;
  // The status information of known nodes of the node is stored in meta
  Meta _meta;
  std::vector<Node*> _nodes;
  // key:node name value:node status
  std::map<std::string, Node*> _status_map;
  // key:node name value:suspicion information
  std::map<std::string, Suspicion*> _suspicion_map;
  // message queue
  BlockingQueue<TransferMessage*> _msg_queue{Config::instance().cluster_message_queue.val()};
  GossipTransport _transport;
  //  GossipTask _gossip_task{*this, _s_config.gossip_interval_us.val(), _s_config.gossip_nodes.val()};
  //  PushPullTask _push_pull_task{*this, _s_config.gossip_interval_us.val(), _s_config.gossip_nodes.val()};
  //  ProbeTask _probe_task{*this, _s_config.gossip_interval_us.val()};
  std::shared_mutex _lock;

  uint64_t probe_index;

  /******** config ********/
  // gossip cycle time
  uint64_t gossip_interval_us;
  // gossip The number of nodes broadcast
  uint64_t gossip_nodes;
  // Determine the offline cycle
  uint64_t gossip_determine_interval_us;

  uint64_t probe_interval_us;

  uint64_t probe_timeout_us;

  uint64_t push_pull_interval_us;
};

}  // namespace oceanbase::logproxy
