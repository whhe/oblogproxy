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

#ifndef BUILD_OPENSOURCE
#include <DRCMessageFactory.h>
#include "gtest/gtest.h"
#include "common.h"
#include "log.h"
#include "cluster/node.h"
#include "guard.hpp"
#include "common_util.h"

using namespace oceanbase::logproxy;

TEST(MODEL, node)
{
  BinlogEntry obj;
  obj.set_config(new InstanceMeta());
  std::string json = obj.serialize_to_json();
  OMS_INFO(json);
}

TEST(MODEL, node_config)
{
  NodeConfig obj;
  auto* config = new SpecialConfig();
  obj.special_configs().push_back(config);
  std::string json = obj.serialize_to_json();
  OMS_INFO(json);

  NodeConfig obj_2;
  obj_2.deserialize_from_json(json);
  ASSERT_EQ(obj_2.max_instances(), -1);
  OMS_INFO(obj_2.serialize_to_json());

  obj_2.set_node_id("xxxxxxx");
  NodeConfig* obj_3 = dynamic_cast<NodeConfig*>(obj_2.clone());
  OMS_INFO(obj_3->serialize_to_json());

  NodeConfig obj_4;
  obj_4.assign(obj_3);
  OMS_INFO(obj_4.serialize_to_json());
  OMS_INFO(obj_3->serialize_to_json());
  ASSERT_STREQ(obj_4.serialize_to_json().c_str(), obj_3->serialize_to_json().c_str());
}

TEST(MODEL, sys_metric)
{
  SysMetric obj;
  std::string json = obj.serialize_to_json();
  OMS_INFO(json);

  SysMetric obj_2;
  Memory* memory = new Memory();
  obj_2.set_memory_status(memory);
  obj_2.deserialize_from_json(json);
  ASSERT_EQ(obj_2.port(), 0);
  OMS_INFO(obj_2.serialize_to_json());

  obj_2.set_ip("xxxxxxx");
  SysMetric* obj_3 = dynamic_cast<SysMetric*>(obj_2.clone());
  OMS_INFO(obj_3->serialize_to_json());
  obj_3->set_memory_status(memory);

  SysMetric obj_21;
  obj_21.assign(obj_3);
  OMS_INFO("assgin:{}", obj_21.serialize_to_json());

  SysMetric obj_4;
  obj_4.assign(obj_3);
  OMS_INFO(obj_4.serialize_to_json());
  OMS_INFO(obj_3->serialize_to_json());
  ASSERT_STREQ(obj_4.serialize_to_json().c_str(), obj_3->serialize_to_json().c_str());
}

TEST(MODEL, clone)
{

  SysMetric obj_2;
  Memory* memory = new Memory();
  obj_2.set_memory_status(memory);
  ASSERT_EQ(obj_2.port(), 0);
  OMS_INFO(obj_2.serialize_to_json());

  obj_2.set_ip("xxxxxxx");
  SysMetric* obj_3 = dynamic_cast<SysMetric*>(obj_2.clone());
  OMS_INFO(obj_3->serialize_to_json());

  SysMetric* obj_4 = dynamic_cast<SysMetric*>(obj_3->clone());
  OMS_INFO(obj_4->serialize_to_json());

  ProcessGroupMetric* process_group_metric = new ProcessGroupMetric();
  ProcessMetric* process_metric = new ProcessMetric();
  process_group_metric->metric_group().push_back(process_metric);
  obj_4->set_process_group_metric(process_group_metric);
  OMS_INFO(obj_4->serialize_to_json());
}

TEST(Node, release_vector)
{
  std::vector<Node*> nodes;
  Node* node = new Node();
  SysMetric* metric = new SysMetric();
  node->set_metric(metric);
  nodes.push_back(node);
  defer(release_vector(nodes));
  sleep(1);
  OMS_INFO("end");
}

TEST(Record, record)
{
  size_t size = 0;
  const char* record_str = NULL;
  IBinlogRecord* binlog_record = DRCMessageFactory::createBinlogRecord(DRCMessageFactory::DFT_BR, true);
  binlog_record->setRecordType(254);
  OMS_INFO("Constructing special record successfully");
  DrcMsgBuf* dmb = new DrcMsgBuf();
  record_str = binlog_record->toString(&size, dmb, true);
  OMS_INFO("Serialization record successful:{}", record_str);

  BinlogRecordImpl* binlog_record_parser =
      (BinlogRecordImpl*)(DRCMessageFactory::createBinlogRecord(DRCMessageFactory::DFT_BR, false));
  int ret = binlog_record_parser->parse(record_str, size);
  if (0 != ret) {
    OMS_ERROR("failed to parse binlog record. record_len:{}, ret:{}", size, ret);
  }
  ASSERT_EQ(binlog_record_parser->recordType(), 254);
  DRCMessageFactory::destroy(binlog_record);
  delete dmb;
}

TEST(SysMetric, se)
{
  std::string json = "{\n"
                     "        \"cpu_status\" : \n"
                     "        {\n"
                     "                \"cpu_count\" : 33,\n"
                     "                \"cpu_used_ratio\" : 0.0\n"
                     "        },\n"
                     "        \"disk_status\" : \n"
                     "        {\n"
                     "                \"disk_total_size_mb\" : 0,\n"
                     "                \"disk_usage_size_process_mb\" : 0,\n"
                     "                \"disk_used_ratio\" : 0.0,\n"
                     "                \"disk_used_size_mb\" : 0\n"
                     "        },\n"
                     "        \"ip\" : \"\",\n"
                     "        \"load_status\" : \n"
                     "        {\n"
                     "                \"load_1\" : 0.0,\n"
                     "                \"load_15\" : 0.0,\n"
                     "                \"load_5\" : 0.0\n"
                     "        },\n"
                     "        \"memory_status\" : \n"
                     "        {\n"
                     "                \"mem_total_size_mb\" : 0,\n"
                     "                \"mem_used_ratio\" : 0.0,\n"
                     "                \"mem_used_size_mb\" : 0\n"
                     "        },\n"
                     "        \"network_status\" : \n"
                     "        {\n"
                     "                \"network_rx_bytes\" : 0,\n"
                     "                \"network_wx_bytes\" : 0\n"
                     "        },\n"
                     "        \"port\" : 12,\n"
                     "        \"process_group_metric\" : null\n"
                     "}";

  Node node;
  SysMetric* sys_metric = new SysMetric(true);
  sys_metric->deserialize_from_json(json);
  OMS_INFO(sys_metric->serialize_to_json());
  OMS_INFO(sys_metric->cpu_status()->serialize_to_json());
  rapidjson::Document document;
  rapidjson::Value value = sys_metric->serialize_to_json_value(document);
  ASSERT_EQ(value["port"].GetInt(), 12);

  std::string process_json =
      "{\"ip\":\"\",\"port\":0,\"cpu_status\":{\"cpu_count\":16,\"cpu_used_ratio\":0.09365352243185043},\"disk_"
      "status\":{\"disk_total_size_mb\":906099,\"disk_usage_size_process_mb\":4474,\"disk_used_ratio\":0."
      "6361357569694519,\"disk_used_size_mb\":576402},\"load_status\":{\"load_1\":1.6799999475479126,\"load_15\":0,"
      "\"load_5\":3.7799999713897705},\"memory_status\":{\"mem_total_size_mb\":128397,\"mem_used_ratio\":0."
      "47995495796203613,\"mem_used_size_mb\":61625},\"network_status\":{\"network_rx_bytes\":1240653,\"network_wx_"
      "bytes\":1162795},\"process_group_metric\":{\"metric_group\":[{\"client_id\":\"/data/huaqing/oms-logproxy/"
      "packenv/"
      "oblogproxy\",\"pid\":12118,\"cpu_status\":{\"cpu_count\":16,\"cpu_used_ratio\":0.06345177441835403},\"disk_"
      "status\":{\"disk_total_size_mb\":906099,\"disk_usage_size_process_mb\":4474,\"disk_used_ratio\":0."
      "6361357569694519,\"disk_used_size_mb\":576402},\"memory_status\":{\"mem_total_size_mb\":128397,\"mem_used_"
      "ratio\":0,\"mem_used_size_mb\":67},\"network_status\":{\"network_rx_bytes\":0,\"network_wx_bytes\":0}},{"
      "\"client_id\":\"/data/huaqing/oms-logproxy/packenv/oblogproxy/run/binlogit/ob_mysql/"
      "binlogit$ob_mysql$2\",\"pid\":47269,\"cpu_status\":{\"cpu_count\":16,\"cpu_used_ratio\":0.1270648092031479},"
      "\"disk_status\":{\"disk_total_size_mb\":906099,\"disk_usage_size_process_mb\":0,\"disk_used_ratio\":0."
      "6361357569694519,\"disk_used_size_mb\":576402},\"memory_status\":{\"mem_total_size_mb\":128397,\"mem_used_"
      "ratio\":0,\"mem_used_size_mb\":2042},\"network_status\":{\"network_rx_bytes\":0,\"network_wx_bytes\":0}}]}}";
  auto* pm = new ProcessMetric(true);
  sys_metric->process_group_metric()->metric_group().push_back(pm);
  sys_metric->deserialize_from_json(process_json);

  OMS_INFO(sys_metric->serialize_to_json());
}

TEST(LIST, to_josn)
{
  NodeConfig obj;
  obj.set_node_id(CommonUtils::generate_trace_id());
  auto* config = new SpecialConfig();
  config->set_cluster_id("ob_test");
  auto* config_2 = new SpecialConfig();
  obj.special_configs().push_back(config);
  obj.special_configs().push_back(config_2);
  std::string json = obj.serialize_to_json();
  OMS_INFO(json);
  auto obj2 = NodeConfig();
  auto* config_3 = new SpecialConfig();
  obj2.special_configs().push_back(config_3);
  obj2.deserialize_from_json(json);
  OMS_INFO(obj2.serialize_to_json());
  OMS_INFO("special_configs_list_type:{}", obj2.special_configs_list_type().name);
  OMS_INFO("special_configs_list_item:{}", obj2.list_item().serialize_to_json());
}

TEST(InstanceMeta, to_json)
{
  InstanceMeta meta;
  rapidjson::Document document;
  auto json = meta.serialize_to_json_value(document);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  json.Accept(writer);
  OMS_INFO("json:{}", buffer.GetString());
  ASSERT_EQ(json["send_interval_ms"].GetInt(), 100);
}

#endif
