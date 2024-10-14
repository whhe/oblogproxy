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

#include <bitset>
#include "gtest/gtest.h"
#include "common.h"
#include "log.h"
#include "binlog/binlog-instance/binlog_convert.h"
#include "binlog/data_type.h"

#include <common_util.h>

using namespace oceanbase::binlog;

TEST(WriteRowsEvent, deserialize)
{
  unsigned char event[] = {0x27,
      0x21,
      0x18,
      0x64,
      0x1e,
      0x44,
      0x0f,
      0x65,
      0x44,
      0x20,
      0x00,
      0x00,
      0x00,
      0x9f,
      0x01,
      0x00,
      0x00,
      0x01,
      0x00,
      0xe0,
      0x3d,
      0x09,
      0x0b,
      0xe7,
      0x63,
      0x01,
      0x00,
      0x02,
      0x00,
      0x01,
      0xff,
      0xff};
  WriteRowsEvent write_rows_event = WriteRowsEvent(0, 0);
  write_rows_event.set_checksum_flag(OFF);
  write_rows_event.deserialize(event);
  printf("%s", write_rows_event.print_event_info().c_str());
  ASSERT_EQ("table_id: 109843973750240 flags: STMT_END_F", write_rows_event.print_event_info());
}

TEST(PreviousGtidsLogEvent, deserialize)
{
  unsigned char event[] = {0x41,
      0xc6,
      0x5a,
      0x65,
      0x23,
      0x01,
      0x00,
      0x00,
      0x00,
      0x47,
      0x00,
      0x00,
      0x00,
      0xc2,
      0x00,
      0x00,
      0x00,
      0x80,
      0x00,
      0x01,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x70,
      0x63,
      0x48,
      0xf0,
      0x07,
      0xfc,
      0x11,
      0xed,
      0xa7,
      0x17,
      0x02,
      0x42,
      0xac,
      0x11,
      0x00,
      0x02,
      0x01,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x01,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x0b,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0xa4,
      0x9a,
      0x91,
      0x03};
  auto pre_gtid_event = PreviousGtidsLogEvent();
  pre_gtid_event.deserialize(event);
  OMS_INFO(pre_gtid_event.print_event_info());
  ASSERT_EQ("706348f0-07fc-11ed-a717-0242ac110002:1-10", pre_gtid_event.print_event_info());
}

TEST(QueryEvent, deserialize)
{
  uint32_t ret = 4294967295;
  uint64_t ret1 = 4294967295;
  OMS_INFO("ret:{},ret1:{}", ret, ret1);
  ret1++;
  ret++;
  uint32_t ret3 = static_cast<uint32_t>(ret1);

  OMS_INFO("ret:{},ret1:{},ret3{}", ret, ret1, ret3);

  uint32_t ret4 = 139;
  OMS_INFO("ret:{},ret1:{},ret3{}", (ret4 - ret), ret1, ret3);
}

TEST(Converter, get_dbname_without_tenant)
{
  {
    std::string full_dbname = "t_xxx11.2.2.3&^%$#@!.db&&^^%%$$##..123&&^^";
    std::string tenant_name = "t_xxx11.2.2.3&^%$#@!";
    auto dbname = CommonUtils::get_dbname_without_tenant(full_dbname, tenant_name);
    OMS_INFO(dbname);
    ASSERT_EQ(dbname, "db&&^^%%$$##..123&&^^");
  }

  {
    std::string full_dbname = "t_xxx11.2.2.3&^%$#@!";
    std::string tenant_name = "t_xxx11.2.2.3&^%$#@!";
    auto dbname = CommonUtils::get_dbname_without_tenant(full_dbname, tenant_name);
    OMS_INFO(dbname);
    ASSERT_EQ(dbname, "t_xxx11.2.2.3&^%$#@!");
  }

  {
    std::string full_dbname = "";
    std::string tenant_name = "";
    auto dbname = CommonUtils::get_dbname_without_tenant(full_dbname, tenant_name);
    OMS_INFO(dbname);
    ASSERT_EQ(dbname, "");
  }

  {
    std::string full_dbname = "db&&^^%%$$##..12t_xxx11.2.2.3&^%$#@!3&&^^";
    std::string tenant_name = "t_xxx11.2.2.3&^%$#@!";
    auto dbname = CommonUtils::get_dbname_without_tenant(full_dbname, tenant_name);
    OMS_INFO(dbname);
    ASSERT_EQ(dbname, "db&&^^%%$$##..12t_xxx11.2.2.3&^%$#@!3&&^^");
  }

  {
    std::string full_dbname = "db&&^^%%$$##..12t_xxx11.2.2.3&^%$#@!3&&^^";
    std::string tenant_name = "";
    auto dbname = CommonUtils::get_dbname_without_tenant(full_dbname, tenant_name);
    OMS_INFO(dbname);
    ASSERT_EQ(dbname, "db&&^^%%$$##..12t_xxx11.2.2.3&^%$#@!3&&^^");
  }

  {
    // For full_dbname in the format of cluster_name.tenant_name.db_name, it is illegal and cannot be determined for us.
    // It is expected and will definitely not happen, and the original value will be returned directly.
    std::string full_dbname = "cluster.t_xxx11.2.2.3&^%$#@!.db&&^^%%$$##..123&&^^";
    std::string tenant_name = "t_xxx11.2.2.3&^%$#@!";
    auto dbname = CommonUtils::get_dbname_without_tenant(full_dbname, tenant_name);
    OMS_INFO(dbname);
    ASSERT_EQ(dbname, "cluster.t_xxx11.2.2.3&^%$#@!.db&&^^%%$$##..123&&^^");
  }
}