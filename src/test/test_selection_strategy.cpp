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

#include "gtest/gtest.h"

#include "env.h"
#include "common.h"

using namespace oceanbase::binlog;
using namespace oceanbase::logproxy;

static std::string online_normal_node_json =
    "{\"group\":\"g1\",\"id\":\"947799f322758ff43c67477aa171\",\"incarnation\":0,\"ip\":"
    "\"11.124.9.113\",\"last_"
    "modify\":"
    "0,\"port\":52983,\"region\":\"hangzhou\",\"state\":0,\"zone\":\"hangzhou-a\","
    "\"metric\":{\"ip\":\"\",\"port\":0,"
    "\"cpu_status\":{"
    "\"cpu_count\":16,\"cpu_used_ratio\":0.02861200086772442},\"disk_status\":{\"disk_"
    "total_size_mb\":906099,\"disk_"
    "usage_size_process_mb\":3520,\"disk_used_ratio\":0.636050820350647,\"disk_used_size_"
    "mb\":576325},\"load_status\":{"
    "\"load_1\":0.36000001430511477,\"load_15\":0.0,\"load_5\":1.6299999952316285},"
    "\"memory_status\":{\"mem_total_size_"
    "mb\":128397,\"mem_used_ratio\":0.4718245565891266,\"mem_used_size_mb\":60581},"
    "\"network_status\":{\"network_rx_"
    "bytes\":148394,\"network_wx_bytes\":155871},\"process_group_metric\":{\"metric_"
    "group\":[]}},\"node_config\":{"
    "\"expiration\":10,\"max_connections\":-1,\"max_instances\":-1,\"node_id\":\"\","
    "\"specific\":false,\"special_"
    "configs\":[]}}";

static std::string online_normal_node_json_2 =
    "{\"group\":\"\",\"id\":\"947799f322758ff43c67477aa172\",\"incarnation\":0,\"ip\":"
    "\"11.124.9.114\",\"last_"
    "modify\":"
    "0,\"port\":52983,\"region\":\"shanghai\",\"state\":0,\"zone\":\"shanghai-a\","
    "\"metric\":{\"ip\":\"\",\"port\":0,"
    "\"cpu_status\":{"
    "\"cpu_count\":16,\"cpu_used_ratio\":0.52861200086772442},\"disk_status\":{\"disk_"
    "total_size_mb\":906099,\"disk_"
    "usage_size_process_mb\":3520,\"disk_used_ratio\":0.436050820350647,\"disk_used_size_"
    "mb\":576325},\"load_status\":{"
    "\"load_1\":0.36000001430511477,\"load_15\":0.0,\"load_5\":1.6299999952316285},"
    "\"memory_status\":{\"mem_total_size_"
    "mb\":128397,\"mem_used_ratio\":0.3718245565891266,\"mem_used_size_mb\":60581},"
    "\"network_status\":{\"network_rx_"
    "bytes\":48394,\"network_wx_bytes\":105871},\"process_group_metric\":{\"metric_"
    "group\":[]}},\"node_config\":{"
    "\"expiration\":10,\"max_connections\":-1,\"max_instances\":-1,\"node_id\":\"\","
    "\"specific\":false,\"special_"
    "configs\":[]}}";

static std::string online_exceed_resource_node_json =
    "{\"group\":\"g1\",\"id\":\"947799f322758ff43c67477aa173\",\"incarnation\":0,\"ip\":"
    "\"11.124.9.113\",\"last_"
    "modify\":"
    "0,\"port\":52983,\"region\":\"hangzhou\",\"state\":0,\"zone\":\"hangzhou-a\","
    "\"metric\":{\"ip\":\"\",\"port\":0,"
    "\"cpu_status\":{"
    "\"cpu_count\":16,\"cpu_used_ratio\":0.82861200086772442},\"disk_status\":{\"disk_"
    "total_size_mb\":906099,\"disk_"
    "usage_size_process_mb\":3520,\"disk_used_ratio\":0.836050820350647,\"disk_used_size_"
    "mb\":576325},\"load_status\":{"
    "\"load_1\":0.36000001430511477,\"load_15\":0.0,\"load_5\":1.6299999952316285},"
    "\"memory_status\":{\"mem_total_size_"
    "mb\":128397,\"mem_used_ratio\":0.3718245565891266,\"mem_used_size_mb\":60581},"
    "\"network_status\":{\"network_rx_"
    "bytes\":48394,\"network_wx_bytes\":105871},\"process_group_metric\":{\"metric_"
    "group\":[]}},\"node_config\":{"
    "\"expiration\":10,\"max_connections\":-1,\"max_instances\":-1,\"node_id\":\"\","
    "\"specific\":false,\"special_"
    "configs\":[]}}";

static std::string online_exceed_mim_resource_node_json =
    "{\"group\":\"g1\",\"id\":\"947799f322758ff43c67477aa173\",\"incarnation\":0,\"ip\":"
    "\"11.124.9.113\",\"last_"
    "modify\":"
    "0,\"port\":52983,\"region\":\"hangzhou\",\"state\":0,\"zone\":\"hangzhou-a\","
    "\"metric\":{\"ip\":\"\",\"port\":0,"
    "\"cpu_status\":{"
    "\"cpu_count\":16,\"cpu_used_ratio\":0.22861200086772442},\"disk_status\":{\"disk_"
    "total_size_mb\":606099,\"disk_"
    "usage_size_process_mb\":3520,\"disk_used_ratio\":0.636050820350647,\"disk_used_size_"
    "mb\":576325},\"load_status\":{"
    "\"load_1\":0.36000001430511477,\"load_15\":0.0,\"load_5\":1.6299999952316285},"
    "\"memory_status\":{\"mem_total_size_"
    "mb\":78397,\"mem_used_ratio\":0.3718245565891266,\"mem_used_size_mb\":60581},"
    "\"network_status\":{\"network_rx_"
    "bytes\":48394,\"network_wx_bytes\":105871},\"process_group_metric\":{\"metric_"
    "group\":[]}},\"node_config\":{"
    "\"expiration\":10,\"max_connections\":-1,\"max_instances\":-1,\"node_id\":\"\","
    "\"specific\":false,\"special_"
    "configs\":[]}}";

void add_node(std::vector<Node*>& nodes, const std::string& json_str)
{
  Node* node = new Node(true);
  node->deserialize_from_json(json_str);
  nodes.push_back(node);
}

void init_instance_meta(InstanceMeta& meta, std::map<std::string, std::string>& instance_options)
{
  meta.set_instance_name("binlog$mysql$1");
  meta.set_cluster("binlog");
  meta.set_tenant("mysql");
  meta.init(instance_options);
}

void init_env()
{
  s_config.cluster_mode.set(false);
  s_config.cluster_protocol.set("STANDALONE");
  env_init(16, 16, 8100, 512,512);
}

TEST(SelectionStrategy, load_balancing_node_normal)
{
  init_env();
  // case 1: nodes is empty
  {
    InstanceMeta meta;
    std::vector<Node*> nodes;
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(OMS_FAILED, ret);
  }

  // case 2: normal case
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_OK);
    ASSERT_STREQ(nodes.at(0)->id().c_str(), "947799f322758ff43c67477aa171");
  }

  // case 3: select from 2 normal nodes
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json);
    add_node(nodes, online_normal_node_json_2);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_OK);
    ASSERT_EQ(nodes.size(), 1);
    OMS_INFO("Select node: {}", nodes.at(0)->id());
  }

  // case 4: 1 normal node + 1 not meet resource requirement
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json);
    add_node(nodes, online_exceed_resource_node_json);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_OK);
    ASSERT_STREQ(nodes.at(0)->id().c_str(), "947799f322758ff43c67477aa171");
    OMS_INFO("Select node: {}", nodes.at(0)->id());
  }

  // case 5: select from multiple nodes
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json);
    add_node(nodes, online_normal_node_json_2);
    add_node(nodes, online_normal_node_json);
    nodes.at(nodes.size() - 1)->set_id("id#1");
    add_node(nodes, online_exceed_resource_node_json);
    add_node(nodes, online_exceed_mim_resource_node_json);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_OK);
    OMS_INFO("Select node: {}", nodes.at(0)->id());
  }

  env_deInit();
}

TEST(SelectionStrategy, load_balancing_node_for_ip)
{
  init_env();
  // case 1: ip is specified, and matched 1 node
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    instance_options["ip"] = "11.124.9.113";
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_OK);
    ASSERT_STREQ(nodes.at(0)->id().c_str(), "947799f322758ff43c67477aa171");
  }

  // case 2: ip is specified, and not matched any node
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    instance_options["ip"] = "188.92.23.4";
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_FAILED);
    ASSERT_EQ(nodes.empty(), true);
  }

  // case 3: ip is specified, but not meet resource requirement
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    instance_options["ip"] = "11.124.9.113";
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json_2);
    add_node(nodes, online_exceed_resource_node_json);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_FAILED);
  }

  // case 4: ip is specified, and 2 meet resource requirement
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    instance_options["ip"] = "11.124.9.113";
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json);
    add_node(nodes, online_exceed_resource_node_json);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_OK);
    OMS_INFO("Select node: {}", nodes.at(0)->id());
  }

  env_deInit();
}

TEST(SelectionStrategy, load_balancing_node_for_group)
{
  init_env();
  // case 1: group is specified, and matched 1 node
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    instance_options["group"] = "g1";
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_OK);
    ASSERT_STREQ(nodes.at(0)->id().c_str(), "947799f322758ff43c67477aa171");
  }

  // case 2: group is specified, and not matched any node
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    instance_options["group"] = "g1";
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json_2);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_FAILED);
  }

  // case 3: group is specified, and not meet resource requirement
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    instance_options["group"] = "g1";
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json_2);
    add_node(nodes, online_exceed_resource_node_json);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_FAILED);
  }

  // case 4: group is specified, and select from 3 nodes
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    instance_options["group"] = "g1";
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json);
    add_node(nodes, online_normal_node_json_2);
    nodes.at(nodes.size() - 1)->set_group("g1");
    add_node(nodes, online_normal_node_json);
    nodes.at(nodes.size() - 1)->set_id("8233e12");
    nodes.at(nodes.size() - 1)->metric()->memory_status()->set_mem_used_ratio(0.1);
    add_node(nodes, online_exceed_resource_node_json);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_OK);
    ASSERT_EQ(nodes.size(), 1);
    OMS_INFO("Select node: {}", nodes.at(0)->id());
  }

  env_deInit();
}

TEST(SelectionStrategy, load_balancing_node_for_zone)
{
  init_env();
  // case 1: zone is specified, and matched 1 node
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    instance_options["zone"] = "hangzhou-a";
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json);
    add_node(nodes, online_normal_node_json_2);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_OK);
    ASSERT_STREQ(nodes.at(0)->id().c_str(), "947799f322758ff43c67477aa171");
  }

  // case 2: zone is specified, and matched 1 node
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    instance_options["zone"] = "hangzhou-a";
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json);
    add_node(nodes, online_exceed_resource_node_json);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_OK);
    ASSERT_STREQ(nodes.at(0)->id().c_str(), "947799f322758ff43c67477aa171");
  }

  env_deInit();
}

TEST(SelectionStrategy, load_balancing_node_for_region)
{
  init_env();
  // case 1: region is specified, and matched 1 node
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    instance_options["region"] = "hangzhou";
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json);
    add_node(nodes, online_normal_node_json_2);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_OK);
    ASSERT_STREQ(nodes.at(0)->id().c_str(), "947799f322758ff43c67477aa171");
  }

  // case 2: region is specified, and not matched any node
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    instance_options["region"] = "shenzhen";
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json);
    add_node(nodes, online_normal_node_json_2);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_FAILED);
  }

  // case 3: region is specified, and not meet resource requirement
  {
    InstanceMeta meta(true);
    std::map<std::string, std::string> instance_options;
    instance_options["region"] = "hangzhou";
    init_instance_meta(meta, instance_options);
    std::vector<Node*> nodes;
    defer(release_vector(nodes));
    add_node(nodes, online_normal_node_json_2);
    add_node(nodes, online_exceed_resource_node_json);
    std::string err_msg;
    int ret = SelectionStrategy::load_balancing_node(meta, nodes, err_msg);
    ASSERT_EQ(ret, OMS_FAILED);
  }

  env_deInit();
}

static std::string init_instance_json =
    "{\"cluster\":\"binlogit\",\"delay\":18446744073709551615,\"gtid_seq\":\"\",\"heartbeat\":0,\"instance_name\":"
    "\"binlogit$ob_mysql$1\",\"ip\":\"11.124.9.113\",\"min_dump_checkpoint\":0,\"node_id\":"
    "\"8bf5d28ef770936523db49edb952e67\",\"peer\":\"\",\"pid\":0,\"port\":0,\"state\":0,\"tenant\":\"ob_mysql\",\"work_"
    "path\":\"./run/binlogit/ob_mysql/"
    "binlogit$ob_mysql$1\",\"config\":{\"cluster\":\"binlogit\",\"expiration\":10,\"fd\":-1,\"instance_name\":"
    "\"binlogit$ob_mysql$1\",\"send_interval_ms\":100,\"tenant\":\"ob_mysql\",\"binlog_config\":{\"active_state_after_"
    "boot\":0,\"auto_start_obcdc\":1,\"binlog_expire_logs_seconds\":259200,\"binlog_expire_logs_size\":53687091200,"
    "\"failover\":1,\"initial_ob_txn_gtid_seq\":1,\"initial_ob_txn_id\":\"\",\"master_server_id\":0,\"master_server_"
    "uuid\":\"\",\"max_binlog_size\":536870912,\"server_id\":0,\"server_uuid\":\"\"},\"cdc_config\":{\"extra_obcdc_"
    "cfg\":\"cluster="
    "binlogit enable_convert_timestamp_to_unix_timestamp=1 enable_output_by_table_def=1 "
    "enable_output_hidden_primary_key=0 enable_output_invisible_column=1 enable_output_trans_order_by_sql_operation=1 "
    "first_start_timestamp_us=0 sort_trans_participants=1 tb_white_list=ob_mysql.*.* tenant=ob_mysql "
    "working_mode=storage\",\"memory_limit\":\"3G\",\"rootserver_list\":\"\",\"start_timestamp\":1699533789},\"slot_"
    "config\":{\"group\":\"\",\"ip\":\"\",\"region\":\"\",\"zone\":\"\"}}}";

static std::string running_instance_json =
    "{\"cluster\":\"binlogit\",\"delay\":18446744073709551615,\"gtid_seq\":\"1699529449##1#0|1699529449#1002_"
    "1699533256690328#1#1|1699569345##1#0|1699581237##1#0|1699581247##1#0|1699581257##1#0\",\"heartbeat\":0,\"instance_"
    "name\":"
    "\"binlogit$ob_mysql$1\",\"ip\":\"11.124.9.113\",\"min_dump_checkpoint\":0,\"node_id\":"
    "\"8bf5d28ef770936523db49edb952e67\",\"peer\":\"\",\"pid\":0,\"port\":0,\"state\":2,\"tenant\":\"ob_mysql\",\"work_"
    "path\":\"./run/binlogit/ob_mysql/"
    "binlogit$ob_mysql$1\",\"config\":{\"cluster\":\"binlogit\",\"expiration\":10,\"fd\":-1,\"instance_name\":"
    "\"binlogit$ob_mysql$1\",\"send_interval_ms\":100,\"tenant\":\"ob_mysql\",\"binlog_config\":{\"active_state_after_"
    "boot\":0,\"auto_start_obcdc\":1,\"binlog_expire_logs_seconds\":259200,\"binlog_expire_logs_size\":53687091200,"
    "\"failover\":1,\"initial_ob_txn_gtid_seq\":1,\"initial_ob_txn_id\":\"\",\"master_server_id\":0,\"master_server_"
    "uuid\":\"\",\"max_binlog_size\":536870912,\"server_id\":0,\"server_uuid\":\"\"},\"cdc_config\":{\"extra_obcdc_"
    "cfg\":\"cluster="
    "binlogit enable_convert_timestamp_to_unix_timestamp=1 enable_output_by_table_def=1 "
    "enable_output_hidden_primary_key=0 enable_output_invisible_column=1 enable_output_trans_order_by_sql_operation=1 "
    "first_start_timestamp_us=0 sort_trans_participants=1 tb_white_list=ob_mysql.*.* tenant=ob_mysql "
    "working_mode=storage\",\"memory_limit\":\"3G\",\"rootserver_list\":\"\",\"start_timestamp\":1699533789},\"slot_"
    "config\":{\"group\":\"\",\"ip\":\"\",\"region\":\"\",\"zone\":\"\"}}}";

static std::string running_instance_json_2 =
    "{\"cluster\":\"binlogit\",\"delay\":18446744073709551615,\"gtid_seq\":\"1699529449##1#0|1699529449#1002_"
    "1699533256690328#1#1|1699532449##1#0|1699532459##1#0|1699532469##1#0|1699532479##1#0\",\"heartbeat\":0,\"instance_"
    "name\":"
    "\"binlogit$ob_mysql$2\",\"ip\":\"11.124.9.113\",\"min_dump_checkpoint\":0,\"node_id\":"
    "\"8bf5d28ef770936523db49edb952e67\",\"peer\":\"\",\"pid\":0,\"port\":0,\"state\":2,\"tenant\":\"ob_mysql\",\"work_"
    "path\":\"./run/binlogit/ob_mysql/"
    "binlogit$ob_mysql$1\",\"config\":{\"cluster\":\"binlogit\",\"expiration\":10,\"fd\":-1,\"instance_name\":"
    "\"binlogit$ob_mysql$2\",\"send_interval_ms\":100,\"tenant\":\"ob_mysql\",\"binlog_config\":{\"active_state_after_"
    "boot\":0,\"auto_start_obcdc\":1,\"binlog_expire_logs_seconds\":259200,\"binlog_expire_logs_size\":53687091200,"
    "\"failover\":1,\"initial_ob_txn_gtid_seq\":1,\"initial_ob_txn_id\":\"\",\"master_server_id\":0,\"master_server_"
    "uuid\":\"\",\"max_binlog_size\":536870912,\"server_id\":0,\"server_uuid\":\"\"},\"cdc_config\":{\"extra_obcdc_"
    "cfg\":\"cluster="
    "binlogit enable_convert_timestamp_to_unix_timestamp=1 enable_output_by_table_def=1 "
    "enable_output_hidden_primary_key=0 enable_output_invisible_column=1 enable_output_trans_order_by_sql_operation=1 "
    "first_start_timestamp_us=0 sort_trans_participants=1 tb_white_list=ob_mysql.*.* tenant=ob_mysql "
    "working_mode=storage\",\"memory_limit\":\"3G\",\"rootserver_list\":\"\",\"start_timestamp\":1699533789},\"slot_"
    "config\":{\"group\":\"\",\"ip\":\"\",\"region\":\"\",\"zone\":\"\"}}}";

static std::string running_instance_json_3 =
    "{\"cluster\":\"binlogit\",\"delay\":18446744073709551615,\"gtid_seq\":\"1699581237##1#0|1699581247##1#0|"
    "1699581257##1#0\",\"heartbeat\":0,\"instance_name\":"
    "\"binlogit$ob_mysql$3\",\"ip\":\"11.124.9.113\",\"min_dump_checkpoint\":0,\"node_id\":"
    "\"8bf5d28ef770936523db49edb952e67\",\"peer\":\"\",\"pid\":0,\"port\":0,\"state\":2,\"tenant\":\"ob_mysql\",\"work_"
    "path\":\"./run/binlogit/ob_mysql/"
    "binlogit$ob_mysql$1\",\"config\":{\"cluster\":\"binlogit\",\"expiration\":10,\"fd\":-1,\"instance_name\":"
    "\"binlogit$ob_mysql$3\",\"send_interval_ms\":100,\"tenant\":\"ob_mysql\",\"binlog_config\":{\"active_state_after_"
    "boot\":0,\"auto_start_obcdc\":1,\"binlog_expire_logs_seconds\":259200,\"binlog_expire_logs_size\":53687091200,"
    "\"failover\":1,\"initial_ob_txn_gtid_seq\":1,\"initial_ob_txn_id\":\"\",\"master_server_id\":0,\"master_server_"
    "uuid\":\"\",\"max_binlog_size\":536870912,\"server_id\":0,\"server_uuid\":\"\"},\"cdc_config\":{\"extra_obcdc_"
    "cfg\":\"cluster="
    "binlogit enable_convert_timestamp_to_unix_timestamp=1 enable_output_by_table_def=1 "
    "enable_output_hidden_primary_key=0 enable_output_invisible_column=1 enable_output_trans_order_by_sql_operation=1 "
    "first_start_timestamp_us=0 sort_trans_participants=1 tb_white_list=ob_mysql.*.* tenant=ob_mysql "
    "working_mode=storage\",\"memory_limit\":\"3G\",\"rootserver_list\":\"\",\"start_timestamp\":1699533789},\"slot_"
    "config\":{\"group\":\"\",\"ip\":\"\",\"region\":\"\",\"zone\":\"\"}}}";

static std::string running_instance_json_4 =
    "{\"cluster\":\"binlogit\",\"delay\":18446744073709551615,\"gtid_seq\":\"\",\"heartbeat\":0,\"instance_name\":"
    "\"binlogit$ob_mysql$4\",\"ip\":\"11.124.9.113\",\"min_dump_checkpoint\":0,\"node_id\":"
    "\"8bf5d28ef770936523db49edb952e67\",\"peer\":\"\",\"pid\":0,\"port\":0,\"state\":2,\"tenant\":\"ob_mysql\",\"work_"
    "path\":\"./run/binlogit/ob_mysql/"
    "binlogit$ob_mysql$1\",\"config\":{\"cluster\":\"binlogit\",\"expiration\":10,\"fd\":-1,\"instance_name\":"
    "\"binlogit$ob_mysql$3\",\"send_interval_ms\":100,\"tenant\":\"ob_mysql\",\"binlog_config\":{\"active_state_after_"
    "boot\":0,\"auto_start_obcdc\":1,\"binlog_expire_logs_seconds\":259200,\"binlog_expire_logs_size\":53687091200,"
    "\"failover\":1,\"initial_ob_txn_gtid_seq\":1,\"initial_ob_txn_id\":\"\",\"master_server_id\":0,\"master_server_"
    "uuid\":\"\",\"max_binlog_size\":536870912,\"server_id\":0,\"server_uuid\":\"\"},\"cdc_config\":{\"extra_obcdc_"
    "cfg\":\"cluster="
    "binlogit enable_convert_timestamp_to_unix_timestamp=1 enable_output_by_table_def=1 "
    "enable_output_hidden_primary_key=0 enable_output_invisible_column=1 enable_output_trans_order_by_sql_operation=1 "
    "first_start_timestamp_us=0 sort_trans_participants=1 tenant=ob_mysql "
    "working_mode=storage\",\"memory_limit\":\"3G\",\"rootserver_list\":\"\",\"start_timestamp\":1699533789},\"slot_"
    "config\":{\"group\":\"\",\"ip\":\"\",\"region\":\"\",\"zone\":\"\"}}}";

int add_instance(std::map<uint8_t, std::vector<BinlogEntry*>>& state_instance_entries, std::string& json_str)
{
  auto instance = new BinlogEntry(true);
  if (OMS_OK != instance->deserialize_from_json(json_str)) {
    return OMS_FAILED;
  }
  state_instance_entries[instance->state()].emplace_back(instance);
  return OMS_OK;
}
//
// TEST(SelectionStrategy, base_instance_with_init_status)
//{
//  // case 1
//  {
//    uint64_t timestamp = 1699533589;
//    std::map<uint8_t, std::vector<BinlogEntry*>> state_instance_entries;
//    ASSERT_EQ(add_instance(state_instance_entries, init_instance_json), OMS_OK);
//
//    BinlogEntry base_instance;
//    SelectionStrategy::select_instance_for_initial_gtid(timestamp, state_instance_entries, base_instance);
//    ASSERT_STREQ(base_instance.instance_name().c_str(), "binlogit$ob_mysql$1");
//  }
//
//  // case 2
//  {
//    uint64_t timestamp = 1699533589;
//    std::map<uint8_t, std::vector<BinlogEntry*>> state_instance_entries;
//    ASSERT_EQ(add_instance(state_instance_entries, init_instance_json), OMS_OK);
//    ASSERT_EQ(add_instance(state_instance_entries, init_instance_json), OMS_OK);
//    state_instance_entries[InstanceState::INIT].back()->set_instance_name("binlogit$ob_mysql$2");
//    state_instance_entries[InstanceState::INIT].back()->config()->cdc_config()->set_start_timestamp(1699533489);
//
//    BinlogEntry base_instance;
//    SelectionStrategy::select_instance_for_initial_gtid(timestamp, state_instance_entries, base_instance);
//    ASSERT_STREQ(base_instance.instance_name().c_str(), "binlogit$ob_mysql$2");
//  }
//}
//
// TEST(SelectionStrategy, base_instance_with_running_instances)
//{
//  // case 1
//  {
//    uint64_t timestamp = 1699579345;
//    std::map<uint8_t, std::vector<BinlogEntry*>> state_instance_entries;
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json), OMS_OK);
//
//    BinlogEntry base_instance;
//    SelectionStrategy::select_instance_for_initial_gtid(timestamp, state_instance_entries, base_instance);
//    ASSERT_STREQ(base_instance.instance_name().c_str(), "binlogit$ob_mysql$1");
//    OMS_INFO("Select base instance: {}", base_instance.serialize_to_json());
//
//    GtidSeq gtid_seq;
//    int ret = binary_search_gtid_seq(timestamp, base_instance.gtid_seq(), gtid_seq);
//    ASSERT_EQ(ret, OMS_OK);
//    ASSERT_STREQ(gtid_seq.serialize().c_str(), "1699569345##1#0");
//  }
//
//  // case 2
//  {
//    uint64_t timestamp = 1699533589;
//    std::map<uint8_t, std::vector<BinlogEntry*>> state_instance_entries;
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json), OMS_OK);
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json_2), OMS_OK);
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json_3), OMS_OK);
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json), OMS_OK);
//    state_instance_entries[InstanceState::RUNNING].back()->set_instance_name("binlogit$ob_mysql$4");
//    state_instance_entries[InstanceState::RUNNING].back()->set_gtid_seq("");
//
//    BinlogEntry base_instance;
//    SelectionStrategy::select_instance_for_initial_gtid(timestamp, state_instance_entries, base_instance);
//    ASSERT_STREQ(base_instance.instance_name().c_str(), "binlogit$ob_mysql$1");
//
//    GtidSeq gtid_seq;
//    int ret = binary_search_gtid_seq(timestamp, base_instance.gtid_seq(), gtid_seq);
//    ASSERT_EQ(ret, OMS_OK);
//    ASSERT_STREQ(gtid_seq.serialize().c_str(), "1699529449#1002_1699533256690328#1#1");
//  }
//
//  // case 3
//  {
//    uint64_t timestamp = 1699533589;
//    std::map<uint8_t, std::vector<BinlogEntry*>> state_instance_entries;
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json), OMS_OK);
//    state_instance_entries[InstanceState::RUNNING].back()->set_instance_name("binlogit$ob_mysql$2");
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json), OMS_OK);
//
//    BinlogEntry base_instance;
//    SelectionStrategy::select_instance_for_initial_gtid(timestamp, state_instance_entries, base_instance);
//    ASSERT_STREQ(base_instance.instance_name().c_str(), "binlogit$ob_mysql$2");
//
//    GtidSeq gtid_seq;
//    int ret = binary_search_gtid_seq(timestamp, base_instance.gtid_seq(), gtid_seq);
//    ASSERT_EQ(ret, OMS_OK);
//    ASSERT_STREQ(gtid_seq.serialize().c_str(), "1699529449#1002_1699533256690328#1#1");
//  }
//
//  // case 4
//  {
//    uint64_t timestamp = 1699533589;
//    std::map<uint8_t, std::vector<BinlogEntry*>> state_instance_entries;
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json_2), OMS_OK);
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json_2), OMS_OK);
//    state_instance_entries[InstanceState::RUNNING].back()->set_instance_name("binlogit$ob_mysql$4");
//    state_instance_entries[InstanceState::RUNNING].back()->set_gtid_seq("1699532529##1#0");
//
//    BinlogEntry base_instance;
//    SelectionStrategy::select_instance_for_initial_gtid(timestamp, state_instance_entries, base_instance);
//    ASSERT_STREQ(base_instance.instance_name().c_str(), "binlogit$ob_mysql$4");
//  }
//
//  // case 5
//  {
//    uint64_t timestamp = 1699533589;
//    std::map<uint8_t, std::vector<BinlogEntry*>> state_instance_entries;
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json_3), OMS_OK);
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json_3), OMS_OK);
//    state_instance_entries[InstanceState::RUNNING].back()->set_instance_name("binlogit$ob_mysql$4");
//    state_instance_entries[InstanceState::RUNNING].back()->set_gtid_seq("1699581227##1#0");
//
//    BinlogEntry base_instance;
//    SelectionStrategy::select_instance_for_initial_gtid(timestamp, state_instance_entries, base_instance);
//    ASSERT_STREQ(base_instance.instance_name().c_str(), "binlogit$ob_mysql$4");
//  }
//
//  // case 6
//  {
//    uint64_t timestamp = 1699533589;
//    std::map<uint8_t, std::vector<BinlogEntry*>> state_instance_entries;
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json), OMS_OK);
//    state_instance_entries[InstanceState::RUNNING].back()->set_gtid_seq("");
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json), OMS_OK);
//    state_instance_entries[InstanceState::RUNNING].back()->set_instance_name("binlogit$ob_mysql$4");
//    state_instance_entries[InstanceState::RUNNING].back()->set_gtid_seq("");
//    state_instance_entries[InstanceState::RUNNING].back()->config()->cdc_config()->set_start_timestamp(1699533489);
//
//    BinlogEntry base_instance;
//    SelectionStrategy::select_instance_for_initial_gtid(timestamp, state_instance_entries, base_instance);
//    ASSERT_STREQ(base_instance.instance_name().c_str(), "binlogit$ob_mysql$4");
//  }
//
//  // case 7
//  {
//    uint64_t timestamp = 1699533589;
//    std::map<uint8_t, std::vector<BinlogEntry*>> state_instance_entries;
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json_2), OMS_OK);
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json_3), OMS_OK);
//    ASSERT_EQ(add_instance(state_instance_entries, running_instance_json), OMS_OK);
//    state_instance_entries[InstanceState::RUNNING].back()->set_instance_name("binlogit$ob_mysql$4");
//    state_instance_entries[InstanceState::RUNNING].back()->set_gtid_seq("");
//
//    BinlogEntry base_instance;
//    SelectionStrategy::select_instance_for_initial_gtid(timestamp, state_instance_entries, base_instance);
//    ASSERT_STREQ(base_instance.instance_name().c_str(), "binlogit$ob_mysql$2");
//
//    GtidSeq gtid_seq;
//    int ret = binary_search_gtid_seq(timestamp, base_instance.gtid_seq(), gtid_seq);
//    ASSERT_EQ(ret, OMS_OK);
//    ASSERT_STREQ(gtid_seq.serialize().c_str(), "1699532479##1#0");
//  }
//}

int add_instance(std::vector<BinlogEntry>& instance_entries, std::string& json_str)
{
  auto instance = new BinlogEntry(true);
  if (OMS_OK != instance->deserialize_from_json(json_str)) {
    return OMS_FAILED;
  }
  instance_entries.emplace_back(*instance);
  return OMS_OK;
}

TEST(SelectionStrategy, select_service_instance)
{
  init_env();
  s_config.enable_resource_check.set(false);

  // case 1: empty + empty
  {
    std::vector<BinlogEntry> instance_entries;
    ASSERT_EQ(add_instance(instance_entries, running_instance_json_4), OMS_OK);  // empty
    ASSERT_EQ(add_instance(instance_entries, running_instance_json_4), OMS_OK);  // empty
    instance_entries.back().set_instance_name("binlogit$ob_mysql$5");

    BinlogEntry select_instance;
    std::string err_msg;
    uint64_t min_dump_point = 0;
    ASSERT_EQ(
        OMS_OK, SelectionStrategy::select_master_instance(instance_entries, min_dump_point, select_instance, err_msg));
    ASSERT_STREQ(select_instance.instance_name().c_str(), "binlogit$ob_mysql$4");
  }

  // case 2: meet + empty
  {
    std::vector<BinlogEntry> instance_entries;
    ASSERT_EQ(add_instance(instance_entries, running_instance_json_4), OMS_OK);  // empty
    ASSERT_EQ(add_instance(instance_entries, running_instance_json), OMS_OK);    // [1699529449, 1699581257]

    BinlogEntry select_instance;
    std::string err_msg;
    uint64_t min_dump_point = 0;
    ASSERT_EQ(
        OMS_OK, SelectionStrategy::select_master_instance(instance_entries, min_dump_point, select_instance, err_msg));
    //    ASSERT_STREQ(select_instance.instance_name().c_str(), "binlogit$ob_mysql$1");
  }

  // case 3: empty + empty
  {
    std::vector<BinlogEntry> instance_entries;
    ASSERT_EQ(add_instance(instance_entries, running_instance_json_4), OMS_OK);  // empty
    ASSERT_EQ(add_instance(instance_entries, running_instance_json_4), OMS_OK);  // empty
    instance_entries.back().set_instance_name("binlogit$ob_mysql$5");

    BinlogEntry select_instance;
    std::string err_msg;
    uint64_t min_dump_point = 1699533789;
    ASSERT_EQ(OMS_FAILED,
        SelectionStrategy::select_master_instance(instance_entries, min_dump_point, select_instance, err_msg));
  }

  // case 4: not meet + not meet
  {
    std::vector<BinlogEntry> instance_entries;
    ASSERT_EQ(add_instance(instance_entries, running_instance_json_3), OMS_OK);  // [1699581237, 1699581257]
    ASSERT_EQ(add_instance(instance_entries, running_instance_json_3), OMS_OK);  // [1699581237, 1699581257]

    BinlogEntry select_instance;
    std::string err_msg;
    uint64_t min_dump_point = 1699533789;
    ASSERT_EQ(OMS_FAILED,
        SelectionStrategy::select_master_instance(instance_entries, min_dump_point, select_instance, err_msg));
  }

  // case 5: not meet + empty
  {
    std::vector<BinlogEntry> instance_entries;
    ASSERT_EQ(add_instance(instance_entries, running_instance_json_3), OMS_OK);  // [1699581237, 1699581257]
    ASSERT_EQ(add_instance(instance_entries, running_instance_json_4), OMS_OK);  // empty

    BinlogEntry select_instance;
    std::string err_msg;
    uint64_t min_dump_point = 1699533789;
    ASSERT_EQ(OMS_FAILED,
        SelectionStrategy::select_master_instance(instance_entries, min_dump_point, select_instance, err_msg));
  }

  // case 6: not meet + meet
  {
    std::vector<BinlogEntry> instance_entries;
    ASSERT_EQ(add_instance(instance_entries, running_instance_json_3), OMS_OK);  // [1699581237, 1699581257]
    ASSERT_EQ(add_instance(instance_entries, running_instance_json), OMS_OK);    // [1699529449, 1699581257]

    BinlogEntry select_instance;
    std::string err_msg;
    uint64_t min_dump_point = 1699533789;
    ASSERT_EQ(
        OMS_OK, SelectionStrategy::select_master_instance(instance_entries, min_dump_point, select_instance, err_msg));
    ASSERT_STREQ(select_instance.instance_name().c_str(), "binlogit$ob_mysql$1");
  }

  // case 7: meet + empty
  {
    std::vector<BinlogEntry> instance_entries;
    ASSERT_EQ(add_instance(instance_entries, running_instance_json), OMS_OK);    // [1699529449, 1699581257]
    ASSERT_EQ(add_instance(instance_entries, running_instance_json_4), OMS_OK);  // empty

    BinlogEntry select_instance;
    std::string err_msg;
    uint64_t min_dump_point = 1699533789;
    ASSERT_EQ(
        OMS_OK, SelectionStrategy::select_master_instance(instance_entries, min_dump_point, select_instance, err_msg));
    ASSERT_STREQ(select_instance.instance_name().c_str(), "binlogit$ob_mysql$1");
  }

  // case 8: meet + meet
  {
    std::vector<BinlogEntry> instance_entries;
    ASSERT_EQ(add_instance(instance_entries, running_instance_json_2), OMS_OK);  // [1699529449, 1699532479]
    ASSERT_EQ(add_instance(instance_entries, running_instance_json), OMS_OK);    // [1699529449, 1699581257]

    BinlogEntry select_instance;
    std::string err_msg;
    uint64_t min_dump_point = 1699533789;
    ASSERT_EQ(
        OMS_OK, SelectionStrategy::select_master_instance(instance_entries, min_dump_point, select_instance, err_msg));
    ASSERT_STREQ(select_instance.instance_name().c_str(), "binlogit$ob_mysql$1");
  }

  // case 9: meet + meet
  {
    std::vector<BinlogEntry> instance_entries;
    ASSERT_EQ(add_instance(instance_entries, running_instance_json), OMS_OK);  // [1699529449, 1699581257]
    ASSERT_EQ(add_instance(instance_entries, running_instance_json), OMS_OK);  // [1699529449, 1699581257]
    instance_entries.back().set_instance_name("binlogit$ob_mysql$5");

    BinlogEntry select_instance;
    std::string err_msg;
    uint64_t min_dump_point = 1699533789;
    ASSERT_EQ(
        OMS_OK, SelectionStrategy::select_master_instance(instance_entries, min_dump_point, select_instance, err_msg));
    ASSERT_STREQ(select_instance.instance_name().c_str(), "binlogit$ob_mysql$1");
  }

  env_deInit();
}