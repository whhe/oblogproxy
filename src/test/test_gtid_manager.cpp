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

TEST(GtidSeqManager, save_and_get_and_purge_gtid_seq)
{
  FsUtil::remove(GTID_SEQ_FILENAME, false);
  instance_env_init(32, 0);
  s_config.gtid_seq_compressed_trx_size.set(3);
  s_config.gtid_seq_compressed_interval_s.set(5);
  s_meta.binlog_config()->set_server_uuid("89fbcea2-db65-11e7-a851-fa163e618bac");

  // 1. init
  GtidSeq gtid_seq;
  int ret = g_gtid_manager->get_latest_gtid_seq(gtid_seq);
  ASSERT_EQ(OMS_FAILED, ret);

  std::string gtids_str;
  g_gtid_manager->get_instance_incr_gtids_string(0, gtids_str);
  ASSERT_EQ("", gtids_str);

  // 2. mark_and_compress_case_1: heartbeat
  std::pair<std::string, uint64_t> trx_mapping_1("xid", 1);
  for (int i = 0; i <= 12; i++) {
    g_gtid_manager->mark_event(true, i * 1000000, trx_mapping_1);
    g_gtid_manager->compress_and_save_gtid_seq(trx_mapping_1);
  }
  g_gtid_manager->get_latest_gtid_seq(gtid_seq);
  ASSERT_STREQ(gtid_seq.serialize().c_str(), "10##1#0");
  g_gtid_manager->get_instance_incr_gtids_string(0, gtids_str);
  ASSERT_STREQ(gtids_str.c_str(), "5##1#0|10##1#0");

  // 2. mark_and_compress_case_2: gtid event
  std::pair<std::string, uint64_t> trx_mapping_2;
  for (int i = 12; i <= 20; i++) {
    g_gtid_manager->mark_event(false, i * 1000000, trx_mapping_2);
    trx_mapping_2 = std::pair<std::string, uint64_t>("xid" + std::to_string(i), i - 10);
    g_gtid_manager->compress_and_save_gtid_seq(trx_mapping_2);
  }
  g_gtid_manager->get_latest_gtid_seq(gtid_seq);
  ASSERT_STREQ(gtid_seq.serialize().c_str(), "20#xid20#10#3");
  g_gtid_manager->get_instance_incr_gtids_string(0, gtids_str);
  ASSERT_STREQ(gtids_str.c_str(), "5##1#0|10##1#0|14#xid14#4#3|17#xid17#7#3|20#xid20#10#3");
  for (int i = 20; i <= 31; i++) {
    g_gtid_manager->mark_event(true, i * 1000000, trx_mapping_2);
    g_gtid_manager->compress_and_save_gtid_seq(trx_mapping_2);
  }
  g_gtid_manager->get_instance_incr_gtids_string(0, gtids_str);
  ASSERT_STREQ(gtids_str.c_str(), "5##1#0|10##1#0|14#xid14#4#3|17#xid17#7#3|20#xid20#10#3|25##10#0|30##10#0");

  GtidSeq closest_gtid_seq;
  g_gtid_manager->get_first_le_gtid_seq(10, closest_gtid_seq);
  ASSERT_STREQ(closest_gtid_seq.serialize().c_str(), "20#xid20#10#3");

  // 3. pured gtid seq
  g_gtid_manager->purge_gtid_before(5);
  g_gtid_manager->get_instance_incr_gtids_string(0, gtids_str);
  ASSERT_STREQ(gtids_str.c_str(), "17#xid17#7#3|20#xid20#10#3|25##10#0|30##10#0");

  std::string value;
  g_sys_var->get_global_var("gtid_purged", value);
  ASSERT_STREQ(value.c_str(), "89fbcea2-db65-11e7-a851-fa163e618bac:1-5");

  // 2. mark_and_compress_case_3: gtid event + heartbeat
  // event -> event -> heartbeat
  std::pair<std::string, uint64_t> trx_mapping_3;
  for (int i = 21; i <= 22; i++) {
    g_gtid_manager->mark_event(false, i * 1000000, trx_mapping_3);
    trx_mapping_3 = std::pair<std::string, uint64_t>("xid" + std::to_string(i), i - 10);
    g_gtid_manager->compress_and_save_gtid_seq(trx_mapping_3);
  }
  g_gtid_manager->mark_event(true, 25 * 1000000, trx_mapping_3);
  g_gtid_manager->compress_and_save_gtid_seq(trx_mapping_3);

  g_gtid_manager->get_latest_gtid_seq(gtid_seq);
  ASSERT_STREQ(gtid_seq.serialize().c_str(), "22#xid22#12#2");
  g_gtid_manager->get_instance_incr_gtids_string(0, gtids_str);
  ASSERT_STREQ(gtids_str.c_str(), "17#xid17#7#3|20#xid20#10#3|25##10#0|30##10#0|22#xid22#12#2");
  g_gtid_manager->get_first_le_gtid_seq(12, closest_gtid_seq);
  ASSERT_STREQ(closest_gtid_seq.serialize().c_str(), "22#xid22#12#2");
  ASSERT_EQ(g_gtid_manager->get_first_le_gtid_seq(5, closest_gtid_seq), OMS_FAILED);
  g_gtid_manager->get_first_le_gtid_seq(10, closest_gtid_seq);
  ASSERT_STREQ(closest_gtid_seq.serialize().c_str(), "20#xid20#10#3");

  // event -> heartbeat -> event
  std::pair<std::string, uint64_t> trx_mapping_4;
  g_gtid_manager->mark_event(false, 26 * 1000000, trx_mapping_4);
  trx_mapping_4 = std::pair<std::string, uint64_t>("xid" + std::to_string(26), 26 - 10);
  g_gtid_manager->compress_and_save_gtid_seq(trx_mapping_4);

  g_gtid_manager->mark_event(true, 28 * 1000000, trx_mapping_4);
  g_gtid_manager->compress_and_save_gtid_seq(trx_mapping_4);

  g_gtid_manager->mark_event(false, 30 * 1000000, trx_mapping_4);
  trx_mapping_4 = std::pair<std::string, uint64_t>("xid" + std::to_string(30), 30 - 10);
  g_gtid_manager->compress_and_save_gtid_seq(trx_mapping_4);

  g_gtid_manager->get_latest_gtid_seq(gtid_seq);
  ASSERT_STREQ(gtid_seq.serialize().c_str(), "30#xid30#20#2");
  g_gtid_manager->get_instance_incr_gtids_string(0, gtids_str);
  ASSERT_STREQ(gtids_str.c_str(), "17#xid17#7#3|20#xid20#10#3|25##10#0|30##10#0|22#xid22#12#2|30#xid30#20#2");

  // 3. purge gtid seq
  g_gtid_manager->purge_gtid_before(10);
  g_gtid_manager->get_instance_incr_gtids_string(0, gtids_str);
  ASSERT_STREQ(gtids_str.c_str(), "22#xid22#12#2|30#xid30#20#2");

  g_sys_var->get_global_var("gtid_purged", value);
  ASSERT_STREQ(value.c_str(), "89fbcea2-db65-11e7-a851-fa163e618bac:1-10");

  // 4. init from file
  GtidManager gtid_seq_manager(GTID_SEQ_FILENAME);
  gtid_seq_manager.init();
  g_gtid_manager->get_instance_incr_gtids_string(0, gtids_str);
  ASSERT_STREQ(gtids_str.c_str(), "22#xid22#12#2|30#xid30#20#2");

  // purge gtid seq
  g_gtid_manager->purge_gtid_before(20);
  g_gtid_manager->get_instance_incr_gtids_string(0, gtids_str);
  ASSERT_STREQ(gtids_str.c_str(), "");

  g_sys_var->get_global_var("gtid_purged", value);
  ASSERT_STREQ(value.c_str(), "89fbcea2-db65-11e7-a851-fa163e618bac:1-20");

  instance_env_deInit();
}

TEST(GtidSeqManager, binary_search_gtid_seq)
{
  {
    GtidSeq gtid_seq;
    std::string tenant_gtids_str = "1701136482##1#0|1701136600##6#0|1701136622##6#0|1701136632##6#0";

    int ret = binary_search_gtid_seq(1701136632, tenant_gtids_str, gtid_seq);
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_STREQ(gtid_seq.serialize().c_str(), "1701136632##6#0");

    ret = binary_search_gtid_seq(1701136482, tenant_gtids_str, gtid_seq);
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_STREQ(gtid_seq.serialize().c_str(), "1701136482##1#0");

    ret = binary_search_gtid_seq(1701136182, tenant_gtids_str, gtid_seq);
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_STREQ(gtid_seq.serialize().c_str(), "1701136482##1#0");
  }
  {
    GtidSeq gtid_seq;
    std::string tenant_gtids_str = "17#xid17#7#3|20#xid20#10#3|22#xid22#12#2|30#xid30#20#2";

    // case 1: boundary
    int ret = binary_search_gtid_seq(15, tenant_gtids_str, gtid_seq);
    ASSERT_EQ(OMS_OK, ret);

    ret = binary_search_gtid_seq(0, tenant_gtids_str, gtid_seq);
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_STREQ(gtid_seq.serialize().c_str(), "30#xid30#20#2");

    ret = binary_search_gtid_seq(35, tenant_gtids_str, gtid_seq);
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_STREQ(gtid_seq.serialize().c_str(), "30#xid30#20#2");

    // case 2: normal
    ret = binary_search_gtid_seq(22, tenant_gtids_str, gtid_seq);
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_STREQ(gtid_seq.serialize().c_str(), "22#xid22#12#2");

    ret = binary_search_gtid_seq(18, tenant_gtids_str, gtid_seq);
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_STREQ(gtid_seq.serialize().c_str(), "17#xid17#7#3");
  }
}

TEST(GtidSeqManager, boundary_gtid_seq)
{
  // case 1
  std::pair<GtidSeq, GtidSeq> gtid_seq_pair1;
  int ret = boundary_gtid_seq("", gtid_seq_pair1);
  ASSERT_EQ(ret, OMS_FAILED);

  // case 2
  std::pair<GtidSeq, GtidSeq> gtid_seq_pair2;
  ret = boundary_gtid_seq("17#xid17#7#3|20#xid20#10#3|22#xid22#12#2|30#xid30#20#2", gtid_seq_pair2);
  ASSERT_EQ(ret, OMS_OK);
  ASSERT_EQ(gtid_seq_pair2.first.get_commit_version_start(), 17);
  ASSERT_EQ(gtid_seq_pair2.second.get_commit_version_start(), 30);

  // case 3
  std::pair<GtidSeq, GtidSeq> gtid_seq_pair3;
  ret = boundary_gtid_seq("17#xid17#7#3", gtid_seq_pair3);
  ASSERT_EQ(ret, OMS_OK);
  ASSERT_EQ(gtid_seq_pair3.first.get_commit_version_start(), 17);
  ASSERT_EQ(gtid_seq_pair3.second.get_commit_version_start(), 17);
}
