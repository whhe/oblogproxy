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
#include "log.h"
#include "blocking_queue.hpp"
#include "sql_parser.h"
#include "sql_cmd_processor.h"
#include "sql/statements.h"

using namespace oceanbase::binlog;
TEST(SQLParser, parser)
{
  hsql::SQLParserResult result;
  std::string query = "show binlog status;";
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  ASSERT_EQ(hsql::COM_SHOW_BINLOG_STAT, result.getStatement(0)->type());
}

TEST(SQLParser, purge)
{
  hsql::SQLParserResult result;
  std::string query = "PURGE BINARY LOGS BEFORE '2019-04-02 22:46:26' FOR TENANT `cluster`.`tenant`";
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  ASSERT_EQ(hsql::COM_PURGE_BINLOG, result.getStatement(0)->type());

  query = "PURGE BINARY LOGS BEFORE '2019-04-02 22:46:26' FOR INSTANCE `cluster$tenant$1`";
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  ASSERT_EQ(hsql::COM_PURGE_BINLOG, result.getStatement(0)->type());

  query = "PURGE BINARY LOGS BEFORE '2019-04-02 22:46:26'";
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  ASSERT_EQ(hsql::COM_PURGE_BINLOG, result.getStatement(0)->type());

  query = "PURGE BINARY LOGS TO 'mysql-bin.000002' FOR TENANT `ob_10088121143.admin`.`binlog`";
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  ASSERT_EQ(hsql::COM_PURGE_BINLOG, result.getStatement(0)->type());

  query = "PURGE BINARY LOGS TO 'mysql-bin.000002' FOR INSTANCE `ob_10088121143.admin$binlog$1`";
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  ASSERT_EQ(hsql::COM_PURGE_BINLOG, result.getStatement(0)->type());
}

TEST(SQLParser, create)
{
  {
    hsql::SQLParserResult result;
    std::string query = "CREATE BINLOG FOR TENANT `cluster`.`tenant` WITH CLUSTER URL `cluster_url`, SERVER UUID "
                        "`2340778c-7464-11ed-a721-7cd30abc99b4`";
    int ret = ObSqlParser::parse(query, result);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result.errorMsg());
    }
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(hsql::COM_CREATE_BINLOG, result.getStatement(0)->type());

    query = "CREATE BINLOG FOR TENANT `cluster`.`tenant` WITH CLUSTER URL "
            "`cluster_url`";
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    ASSERT_EQ(hsql::COM_CREATE_BINLOG, result.getStatement(0)->type());

    query = "CREATE BINLOG IF NOT EXISTS FOR TENANT `cluster`.`tenant` FROM 1678687771255176 WITH CLUSTER URL "
            "`cluster_url`";
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    ASSERT_EQ(hsql::COM_CREATE_BINLOG, result.getStatement(0)->type());
  }
  {
    hsql::SQLParserResult result;
    std::string query = "CREATE BINLOG FOR TENANT `cluster`.`tenant` TO USER `user` PASSWORD `pwd` WITH CLUSTER URL "
                        "`cluster_url`,"
                        "SERVER UUID `2340778c-7464-11ed-a721-7cd30abc99b4`";
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    ASSERT_EQ(hsql::COM_CREATE_BINLOG, result.getStatement(0)->type());

    query = "CREATE BINLOG FOR TENANT `cluster`.`tenant` WITH CLUSTER URL "
            "`cluster_url`";
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    ASSERT_EQ(hsql::COM_CREATE_BINLOG, result.getStatement(0)->type());

    query = "CREATE BINLOG IF NOT EXISTS FOR TENANT `cluster`.`tenant` TO USER `user` PASSWORD `pwd` FROM "
            "1678687771255176 WITH CLUSTER URL "
            "`cluster_url`";
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    ASSERT_EQ(hsql::COM_CREATE_BINLOG, result.getStatement(0)->type());
  }
  {
    hsql::SQLParserResult result;
    std::string query =
        "CREATE BINLOG FOR TENANT `cluster`.`tenant` TO USER `user` PASSWORD `pwd` WITH CLUSTER URL "
        "`cluster_url`,"
        "SERVER UUID `2340778c-7464-11ed-a721-7cd30abc99b4`,INITIAL_TRX_XID `ob_txn_id`,INITIAL_TRX_GTID_SEQ `31`";
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    ASSERT_EQ(hsql::COM_CREATE_BINLOG, result.getStatement(0)->type());
  }
  {
    hsql::SQLParserResult result;
    std::string query =
        "CREATE BINLOG FOR TENANT `cluster`.`tenant` TO USER `user` PASSWORD `pwd` WITH CLUSTER URL "
        "'cluster_url',"
        "SERVER UUID '2340778c-7464-11ed-a721-7cd30abc99b4',INITIAL_TRX_XID '{hash:1380121015845354198, inc:16474501, "
        "addr:\"11.124.9.3:10000\", t:1694412306958599}',INITIAL_TRX_GTID_SEQ '31'";
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    ASSERT_EQ(hsql::COM_CREATE_BINLOG, result.getStatement(0)->type());
  }
  {
    hsql::SQLParserResult result;
    std::string query = "CREATE BINLOG FOR TENANT `ob3x.admin`.`mysql` WITH CLUSTER URL "
                        "`http://xxxx`";
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    ASSERT_EQ(hsql::COM_CREATE_BINLOG, result.getStatement(0)->type());
    auto* create_statement = (hsql::CreateBinlogStatement*)result.getStatement(0);
    ASSERT_STREQ("cluster_url", create_statement->binlog_options->at(0)->column);
    ASSERT_STREQ("http://xxxx", create_statement->binlog_options->at(0)->value->get_value().c_str());
  }
  {
    hsql::SQLParserResult result;
    std::string query = "CREATE BINLOG FOR TENANT `ob3x.admin`.`mysql` WITH CLUSTER URL "
                        "`http://xxxxx`, REPLICATE NUM 3";
    int ret = ObSqlParser::parse(query, result);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result.errorMsg());
    }
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(hsql::COM_CREATE_BINLOG, result.getStatement(0)->type());
    auto* create_statement = (hsql::CreateBinlogStatement*)result.getStatement(0);
    ASSERT_STREQ("cluster_url", create_statement->binlog_options->at(0)->column);
    ASSERT_STREQ("http://xxxxx", create_statement->binlog_options->at(0)->value->get_value().c_str());
    ASSERT_STREQ("replicate_num", create_statement->binlog_options->at(1)->column);
    ASSERT_STREQ(create_statement->binlog_options->at(1)->value->get_value().c_str(), "3");
  }
  {
    hsql::SQLParserResult result;
    std::string query = "CREATE BINLOG FOR TENANT `ob3x.admin`.`mysql` WITH CLUSTER URL "
                        "`http://xxxxx`, throttle_convert_iops = 1, throttle_convert_rps = 2, throttle_dump_conn = 3,"
                        "throttle_dump_iops = 4, throttle_dump_iops = 5";
    int ret = ObSqlParser::parse(query, result);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result.errorMsg());
    }
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(hsql::COM_CREATE_BINLOG, result.getStatement(0)->type());
    auto* create_statement = (hsql::CreateBinlogStatement*)result.getStatement(0);
    ASSERT_STREQ("cluster_url", create_statement->binlog_options->at(0)->column);
    ASSERT_STREQ("http://xxxxx", create_statement->binlog_options->at(0)->value->get_value().c_str());
    ASSERT_STREQ("throttle_convert_iops", create_statement->binlog_options->at(1)->column);
    ASSERT_STREQ(create_statement->binlog_options->at(1)->value->get_value().c_str(), "1");
    ASSERT_STREQ("throttle_convert_rps", create_statement->binlog_options->at(2)->column);
    ASSERT_STREQ(create_statement->binlog_options->at(2)->value->get_value().c_str(), "2");
    ASSERT_STREQ("throttle_dump_conn", create_statement->binlog_options->at(3)->column);
    ASSERT_STREQ(create_statement->binlog_options->at(3)->value->get_value().c_str(), "3");
    ASSERT_STREQ("throttle_dump_iops", create_statement->binlog_options->at(4)->column);
    ASSERT_STREQ(create_statement->binlog_options->at(4)->value->get_value().c_str(), "4");
    ASSERT_STREQ("throttle_dump_iops", create_statement->binlog_options->at(5)->column);
    ASSERT_STREQ(create_statement->binlog_options->at(5)->value->get_value().c_str(), "5");
  }
}

TEST(SQLParser, alter_binlog)
{
  hsql::SQLParserResult result;
  std::string query = "ALTER BINLOG `aa`.`bb` SET BINLOG_EXPIRE_LOGS_SECONDS = 518400;";

  int ret = ObSqlParser::parse(query, result);
  if (OMS_OK != ret) {
    OMS_INFO("parse result: {}", result.errorMsg());
  }

  ASSERT_EQ(OMS_OK, ret);
  ASSERT_EQ(hsql::COM_ALTER_BINLOG, result.getStatement(0)->type());
  auto* alter_statement = (hsql::AlterBinlogStatement*)result.getStatement(0);
  ASSERT_EQ("aa", std::string(alter_statement->tenant->cluster));
  ASSERT_EQ("bb", std::string(alter_statement->tenant->tenant));
  ASSERT_EQ(1, alter_statement->instance_options->size());
  ASSERT_STREQ("binlog_expire_logs_seconds", alter_statement->instance_options->at(0)->column);
  ASSERT_STREQ("518400", alter_statement->instance_options->at(0)->value->get_value().c_str());
}

/*
 * ===================================
 * binlog instance related cases
 * ====================================
 */
TEST(SQLParser, create_binlog_instance)
{
  {
    hsql::SQLParserResult result;
    std::string query =
        "CREATE BINLOG INSTANCE `c_3bqn7192jio$t_jpyg8359m89$138383` FOR `c_3bqn7192jio`.`t_jpyg8359m89` "
        "START_TIMESTAMP = 1693195200, "
        "cluster_url = 'http://configserver.xxx.com/services?Action=ObRootServiceInfo&User_ID=x&UID=y&ObRegion=z';";

    int ret = ObSqlParser::parse(query, result);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result.errorMsg());
    }
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(hsql::COM_CREATE_BINLOG_INSTANCE, result.getStatement(0)->type());
    auto* create_statement = (hsql::CreateBinlogInstanceStatement*)result.getStatement(0);
    ASSERT_EQ("c_3bqn7192jio$t_jpyg8359m89$138383", std::string(create_statement->instance_name));
    ASSERT_EQ("c_3bqn7192jio", std::string(create_statement->tenant->cluster));
    ASSERT_EQ("t_jpyg8359m89", std::string(create_statement->tenant->tenant));
    ASSERT_EQ(2, create_statement->instance_options->size());
    ASSERT_EQ("1693195200", create_statement->instance_options->at(0)->value->get_value());
    ASSERT_EQ("http://configserver.xxx.com/services?Action=ObRootServiceInfo&User_ID=x&UID=y&ObRegion=z",
        create_statement->instance_options->at(1)->value->get_value());
  }
  {
    hsql::SQLParserResult result;
    std::string query =
        "CREATE BINLOG INSTANCE `c_3bqn7192jio$t_jpyg8359m89$138383` FOR `c_3bqn7192jio`.`t_jpyg8359m89` "
        "ip = '11:12:11:1', "
        "cluster_url = 'http://configserver.xxx.com/services?Action=ObRootServiceInfo&User_ID=x&UID=y&ObRegion=z';";

    int ret = ObSqlParser::parse(query, result);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result.errorMsg());
    }
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(hsql::COM_CREATE_BINLOG_INSTANCE, result.getStatement(0)->type());
    auto* create_statement = (hsql::CreateBinlogInstanceStatement*)result.getStatement(0);
    ASSERT_EQ("c_3bqn7192jio$t_jpyg8359m89$138383", std::string(create_statement->instance_name));
    ASSERT_EQ("c_3bqn7192jio", std::string(create_statement->tenant->cluster));
    ASSERT_EQ("t_jpyg8359m89", std::string(create_statement->tenant->tenant));
    ASSERT_EQ(2, create_statement->instance_options->size());
    ASSERT_EQ("11:12:11:1", create_statement->instance_options->at(0)->value->get_value());
    ASSERT_EQ("http://configserver.xxx.com/services?Action=ObRootServiceInfo&User_ID=x&UID=y&ObRegion=z",
        create_statement->instance_options->at(1)->value->get_value());
  }
  {
    hsql::SQLParserResult result;
    std::string query =
        "CREATE BINLOG INSTANCE `c_3bqn7192jio$t_jpyg8359m89$138383` FOR `c_3bqn7192jio`.`t_jpyg8359m89` "
        "ip = '11:12:11:1', "
        "cluster_url = 'http://configserver.xxx.com/services?Action=ObRootServiceInfo&User_ID=x&UID=y&ObRegion=z',"
        "_INITIAL_OB_TXN_ID = 'dada', _INITIAL_OB_TXN_GTID_SEQ = 5;";

    int ret = ObSqlParser::parse(query, result);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result.errorMsg());
    }
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(hsql::COM_CREATE_BINLOG_INSTANCE, result.getStatement(0)->type());
    auto* create_statement = (hsql::CreateBinlogInstanceStatement*)result.getStatement(0);
    ASSERT_EQ("c_3bqn7192jio$t_jpyg8359m89$138383", std::string(create_statement->instance_name));
    ASSERT_EQ("c_3bqn7192jio", std::string(create_statement->tenant->cluster));
    ASSERT_EQ("t_jpyg8359m89", std::string(create_statement->tenant->tenant));
    ASSERT_EQ(4, create_statement->instance_options->size());
    ASSERT_EQ("11:12:11:1", create_statement->instance_options->at(0)->value->get_value());
    ASSERT_EQ("http://configserver.xxx.com/services?Action=ObRootServiceInfo&User_ID=x&UID=y&ObRegion=z",
        create_statement->instance_options->at(1)->value->get_value());
    ASSERT_STREQ("dada", create_statement->instance_options->at(2)->value->get_value().c_str());
    ASSERT_STREQ("5", create_statement->instance_options->at(3)->value->get_value().c_str());
  }
  {
    hsql::SQLParserResult result;
    std::string query =
        "CREATE BINLOG INSTANCE `c_3bqn7192jio$t_jpyg8359m89$138383` FOR `c_3bqn7192jio`.`t_jpyg8359m89` "
        "ip = '11:12:11:1', "
        "cluster_url = 'http://configserver.xxx.com/services?Action=ObRootServiceInfo&User_ID=x&UID=y&ObRegion=z',"
        "MASTER_SERVER_ID=1,MASTER_SERVER_UUID='801b0874-4ae9-11ee-a6e0-acde48001122',"
        "SERVER_ID=2, SERVER_UUID = '8a94f357-aab4-11df-86ab-c80aa9429562', BINLOG_EXPIRE_LOGS_SECONDS = 259200,"
        "BINLOG_EXPIRE_LOGS_SIZE = 53687091200, MAX_BINLOG_SIZE = 536870912, FAILOVER=0, _INITIAL_OB_TXN_ID = 'dada', "
        "_INITIAL_OB_TXN_GTID_SEQ = 5, throttle_convert_iops = 1, throttle_convert_rps = 2, throttle_dump_conn = 3, "
        "throttle_dump_iops = 4, throttle_dump_iops = 5;";

    int ret = ObSqlParser::parse(query, result);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result.errorMsg());
    }
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(hsql::COM_CREATE_BINLOG_INSTANCE, result.getStatement(0)->type());
    auto* create_statement = (hsql::CreateBinlogInstanceStatement*)result.getStatement(0);
    ASSERT_EQ("c_3bqn7192jio$t_jpyg8359m89$138383", std::string(create_statement->instance_name));
    ASSERT_EQ("c_3bqn7192jio", std::string(create_statement->tenant->cluster));
    ASSERT_EQ("t_jpyg8359m89", std::string(create_statement->tenant->tenant));
    ASSERT_EQ(17, create_statement->instance_options->size());
    ASSERT_EQ("11:12:11:1", create_statement->instance_options->at(0)->value->get_value());
    ASSERT_EQ("http://configserver.xxx.com/services?Action=ObRootServiceInfo&User_ID=x&UID=y&ObRegion=z",
        create_statement->instance_options->at(1)->value->get_value());
    ASSERT_STREQ("1", create_statement->instance_options->at(2)->value->get_value().c_str());
    ASSERT_STREQ(
        "801b0874-4ae9-11ee-a6e0-acde48001122", create_statement->instance_options->at(3)->value->get_value().c_str());
    ASSERT_STREQ("2", create_statement->instance_options->at(4)->value->get_value().c_str());
    ASSERT_STREQ(
        "8a94f357-aab4-11df-86ab-c80aa9429562", create_statement->instance_options->at(5)->value->get_value().c_str());
    ASSERT_STREQ("259200", create_statement->instance_options->at(6)->value->get_value().c_str());
    ASSERT_STREQ("53687091200", create_statement->instance_options->at(7)->value->get_value().c_str());
    ASSERT_STREQ("536870912", create_statement->instance_options->at(8)->value->get_value().c_str());
    ASSERT_STREQ("0", create_statement->instance_options->at(9)->value->get_value().c_str());
    ASSERT_STREQ("dada", create_statement->instance_options->at(10)->value->get_value().c_str());
    ASSERT_STREQ("5", create_statement->instance_options->at(11)->value->get_value().c_str());
    ASSERT_STREQ("throttle_convert_iops", create_statement->instance_options->at(12)->column);
    ASSERT_STREQ(create_statement->instance_options->at(12)->value->get_value().c_str(), "1");
    ASSERT_STREQ("throttle_convert_rps", create_statement->instance_options->at(13)->column);
    ASSERT_STREQ(create_statement->instance_options->at(13)->value->get_value().c_str(), "2");
    ASSERT_STREQ("throttle_dump_conn", create_statement->instance_options->at(14)->column);
    ASSERT_STREQ(create_statement->instance_options->at(14)->value->get_value().c_str(), "3");
    ASSERT_STREQ("throttle_dump_iops", create_statement->instance_options->at(15)->column);
    ASSERT_STREQ(create_statement->instance_options->at(15)->value->get_value().c_str(), "4");
    ASSERT_STREQ("throttle_dump_iops", create_statement->instance_options->at(16)->column);
    ASSERT_STREQ(create_statement->instance_options->at(16)->value->get_value().c_str(), "5");
  }
}

TEST(SQLParser, alter_binlog_instance)
{
  hsql::SQLParserResult result;
  std::string query =
      "ALTER BINLOG INSTANCE `c_3bqn7192jio$t_jpyg8359m89$138383` SET BINLOG_EXPIRE_LOGS_SECONDS = 518400;";

  int ret = ObSqlParser::parse(query, result);
  if (OMS_OK != ret) {
    OMS_INFO("parse result: {}", result.errorMsg());
  }

  ASSERT_EQ(OMS_OK, ret);
  ASSERT_EQ(hsql::COM_ALTER_BINLOG_INSTANCE, result.getStatement(0)->type());
  auto* alter_statement = (hsql::AlterBinlogInstanceStatement*)result.getStatement(0);
  ASSERT_EQ("c_3bqn7192jio$t_jpyg8359m89$138383", std::string(alter_statement->instance_name));
  ASSERT_EQ(1, alter_statement->instance_options->size());
  ASSERT_EQ("518400", alter_statement->instance_options->at(0)->value->get_value());
}

TEST(SQLParser, start_binlog_instance)
{
  // case 1
  hsql::SQLParserResult result1;
  std::string sql_none = "START BINLOG INSTANCE `c_3bqn7192jio$t_jpyg8359m89$138383`;";

  int ret = ObSqlParser::parse(sql_none, result1);
  if (OMS_OK != ret) {
    OMS_INFO("parse result: {}", result1.errorMsg());
  }

  ASSERT_EQ(OMS_OK, ret);
  ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_START_BINLOG_INSTANCE);
  auto* start_instance_statement_none = (hsql::StartBinlogInstanceStatement*)result1.getStatement(0);
  ASSERT_EQ(std::string(start_instance_statement_none->instance_name), "c_3bqn7192jio$t_jpyg8359m89$138383");
  ASSERT_EQ(start_instance_statement_none->flag, hsql::InstanceFlag::BOTH);

  // case 2
  hsql::SQLParserResult result2;
  std::string sql_process_only = "START BINLOG INSTANCE `*$9$s` PROCESS_ONLY;";

  ret = ObSqlParser::parse(sql_process_only, result2);
  if (OMS_OK != ret) {
    OMS_INFO("parse result: {}", result2.errorMsg());
  }

  ASSERT_EQ(OMS_OK, ret);
  ASSERT_EQ(result2.getStatement(0)->type(), hsql::COM_START_BINLOG_INSTANCE);
  auto* start_instance_statement_process_only = (hsql::StartBinlogInstanceStatement*)result2.getStatement(0);
  ASSERT_EQ(std::string(start_instance_statement_process_only->instance_name), "*$9$s");
  ASSERT_EQ(start_instance_statement_process_only->flag, hsql::InstanceFlag::PROCESS_ONLY);

  // case 3
  hsql::SQLParserResult result3;
  std::string sql_obcdc_only = "START BINLOG INSTANCE `!@#882` OBCDC_ONLY;";

  ret = ObSqlParser::parse(sql_obcdc_only, result3);
  if (OMS_OK != ret) {
    OMS_INFO("parse result: {}", result3.errorMsg());
  }

  ASSERT_EQ(OMS_OK, ret);
  ASSERT_EQ(result3.getStatement(0)->type(), hsql::COM_START_BINLOG_INSTANCE);
  auto* start_instance_statement_obcdc_only = (hsql::StartBinlogInstanceStatement*)result3.getStatement(0);
  ASSERT_EQ(std::string(start_instance_statement_obcdc_only->instance_name), "!@#882");
  ASSERT_EQ(start_instance_statement_obcdc_only->flag, hsql::InstanceFlag::OBCDC_ONLY);
}

TEST(SQLParser, stop_binlog_instance)
{
  // case 1
  hsql::SQLParserResult result1;
  std::string sql_none = "STOP BINLOG INSTANCE `instance1`;";

  int ret = ObSqlParser::parse(sql_none, result1);
  if (OMS_OK != ret) {
    OMS_INFO("parse result: {}", result1.errorMsg());
  }

  ASSERT_EQ(OMS_OK, ret);
  ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_STOP_BINLOG_INSTANCE);
  auto* stop_instance_statement_none = (hsql::StopBinlogInstanceStatement*)result1.getStatement(0);
  ASSERT_EQ(std::string(stop_instance_statement_none->instance_name), "instance1");
  ASSERT_EQ(stop_instance_statement_none->flag, hsql::InstanceFlag::BOTH);

  // case 2
  hsql::SQLParserResult result2;
  std::string sql_process_only = "STOP BINLOG INSTANCE `aabb` PROCESS_ONLY;";

  ret = ObSqlParser::parse(sql_process_only, result2);
  if (OMS_OK != ret) {
    OMS_INFO("parse result: {}", result2.errorMsg());
  }

  ASSERT_EQ(OMS_OK, ret);
  ASSERT_EQ(result2.getStatement(0)->type(), hsql::COM_STOP_BINLOG_INSTANCE);
  auto* stop_instance_statement_process_only = (hsql::StopBinlogInstanceStatement*)result2.getStatement(0);
  ASSERT_EQ(std::string(stop_instance_statement_process_only->instance_name), "aabb");
  ASSERT_EQ(stop_instance_statement_process_only->flag, hsql::InstanceFlag::PROCESS_ONLY);

  // case 3
  hsql::SQLParserResult result3;
  std::string sql_obcdc_only = "STOP BINLOG INSTANCE `1234` OBCDC_ONLY;";

  ret = ObSqlParser::parse(sql_obcdc_only, result3);
  if (OMS_OK != ret) {
    OMS_INFO("parse result: {}", result3.errorMsg());
  }

  ASSERT_EQ(OMS_OK, ret);
  ASSERT_EQ(result3.getStatement(0)->type(), hsql::COM_STOP_BINLOG_INSTANCE);
  auto* stop_instance_statement_obcdc_only = (hsql::StopBinlogInstanceStatement*)result3.getStatement(0);
  ASSERT_EQ(std::string(stop_instance_statement_obcdc_only->instance_name), "1234");
  ASSERT_EQ(stop_instance_statement_obcdc_only->flag, hsql::InstanceFlag::OBCDC_ONLY);
}

TEST(SQLParser, drop_binlog_instance)
{
  // case 1
  std::string sql = "DROP BINLOG INSTANCE `instance1`;";

  hsql::SQLParserResult result1;
  int ret = ObSqlParser::parse(sql, result1);
  if (OMS_OK != ret) {
    OMS_INFO("parse result: {}", result1.errorMsg());
  }

  ASSERT_EQ(OMS_OK, ret);
  ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_DROP_BINLOG_INSTANCE);
  auto* drop_instance_statement = (hsql::DropBinlogInstanceStatement*)result1.getStatement(0);
  ASSERT_EQ(std::string(drop_instance_statement->instance_name), "instance1");
  ASSERT_EQ(drop_instance_statement->force, false);

  // case 2
  std::string sql_force = "DROP BINLOG INSTANCE `21cs&^` FORCE;";

  hsql::SQLParserResult result2;
  ret = ObSqlParser::parse(sql_force, result2);
  if (OMS_OK != ret) {
    OMS_INFO("parse result: {}", result2.errorMsg());
  }

  ASSERT_EQ(OMS_OK, ret);
  ASSERT_EQ(result2.getStatement(0)->type(), hsql::COM_DROP_BINLOG_INSTANCE);
  auto* drop_instance_statement_force = (hsql::DropBinlogInstanceStatement*)result2.getStatement(0);
  ASSERT_EQ(std::string(drop_instance_statement_force->instance_name), "21cs&^");
  ASSERT_EQ(drop_instance_statement_force->force, true);

  // case 3
  {
    std::string sql_now = "DROP BINLOG INSTANCE `21cs&^` NOW;";

    hsql::SQLParserResult result3;
    ret = ObSqlParser::parse(sql_now, result3);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result3.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result3.getStatement(0)->type(), hsql::COM_DROP_BINLOG_INSTANCE);
    auto* drop_instance_statement_now = (hsql::DropBinlogInstanceStatement*)result3.getStatement(0);
    ASSERT_EQ(std::string(drop_instance_statement_now->instance_name), "21cs&^");
    ASSERT_EQ(drop_instance_statement_now->force, false);
    ASSERT_STREQ(drop_instance_statement_now->defer_execution_sec->get_value().c_str(), "0");
  }

  // case 4
  {
    std::string sql_defer = "DROP BINLOG INSTANCE `21cs&^` FORCE DEFER 600;";

    hsql::SQLParserResult result4;
    ret = ObSqlParser::parse(sql_defer, result4);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result4.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result4.getStatement(0)->type(), hsql::COM_DROP_BINLOG_INSTANCE);
    auto* drop_instance_statement_defer = (hsql::DropBinlogInstanceStatement*)result4.getStatement(0);
    ASSERT_EQ(std::string(drop_instance_statement_defer->instance_name), "21cs&^");
    ASSERT_EQ(drop_instance_statement_defer->force, true);
    ASSERT_STREQ(drop_instance_statement_defer->defer_execution_sec->get_value().c_str(), "600");
  }
}

TEST(SQLParser, show_binlog_instance)
{
  // case 0
  {
    std::string sql = "SHOW HISTORY BINLOG INSTANCE `21cs&^`;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_BINLOG_INSTANCE);
    auto* show_instance_statement = (hsql::ShowBinlogInstanceStatement*)result1.getStatement(0);
    ASSERT_EQ(show_instance_statement->history, true);
    ASSERT_EQ(show_instance_statement->mode, hsql::ShowInstanceMode::INSTANCE);
    ASSERT_EQ(1, show_instance_statement->instance_names->size());
    ASSERT_EQ(std::string(show_instance_statement->instance_names->at(0)), "21cs&^");
  }

  // case 1
  {
    std::string sql = "SHOW BINLOG INSTANCE `21cs&^`;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_BINLOG_INSTANCE);
    auto* show_instance_statement = (hsql::ShowBinlogInstanceStatement*)result1.getStatement(0);
    ASSERT_EQ(show_instance_statement->history, false);
    ASSERT_EQ(show_instance_statement->mode, hsql::ShowInstanceMode::INSTANCE);
    ASSERT_EQ(1, show_instance_statement->instance_names->size());
    ASSERT_EQ(std::string(show_instance_statement->instance_names->at(0)), "21cs&^");
  }

  // case 2
  {
    std::string sql = "SHOW BINLOG INSTANCES limit 3 offset 2;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_BINLOG_INSTANCE);
    auto* show_instance_statement = (hsql::ShowBinlogInstanceStatement*)result1.getStatement(0);
    ASSERT_EQ(show_instance_statement->history, false);
    ASSERT_EQ(show_instance_statement->mode, hsql::ShowInstanceMode::INSTANCE);
    ASSERT_EQ(nullptr, show_instance_statement->instance_names);
    ASSERT_STREQ(show_instance_statement->limit->offset->get_value().c_str(), "2");
    ASSERT_STREQ(show_instance_statement->limit->limit->get_value().c_str(), "3");
  }

  // case 3
  {
    std::string sql =
        "SHOW BINLOG INSTANCES `c_3bqn7192jio$t_jpyg8359m89$138383`, `c_3bqn7192jio$t_jpyg8359m89$138390` limit 2, 3;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_BINLOG_INSTANCE);
    auto* show_instance_statement = (hsql::ShowBinlogInstanceStatement*)result1.getStatement(0);
    ASSERT_EQ(show_instance_statement->history, false);
    ASSERT_EQ(show_instance_statement->mode, hsql::ShowInstanceMode::INSTANCE);
    ASSERT_EQ(2, show_instance_statement->instance_names->size());
    ASSERT_EQ(std::string(show_instance_statement->instance_names->at(0)), "c_3bqn7192jio$t_jpyg8359m89$138383");
    ASSERT_EQ(std::string(show_instance_statement->instance_names->at(1)), "c_3bqn7192jio$t_jpyg8359m89$138390");
    ASSERT_STREQ(show_instance_statement->limit->offset->get_value().c_str(), "2");
    ASSERT_STREQ(show_instance_statement->limit->limit->get_value().c_str(), "3");
  }

  // case 4
  {
    std::string sql = "SHOW BINLOG INSTANCES FOR `c_3bqn7192jio`.`t_jpyg8359m89` LIMIT ALL OFFSET 2;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_BINLOG_INSTANCE);
    auto* show_instance_statement = (hsql::ShowBinlogInstanceStatement*)result1.getStatement(0);
    ASSERT_EQ(show_instance_statement->history, false);
    ASSERT_EQ(show_instance_statement->mode, hsql::ShowInstanceMode::TENANT);
    ASSERT_EQ(std::string(show_instance_statement->tenant->cluster), "c_3bqn7192jio");
    ASSERT_EQ(std::string(show_instance_statement->tenant->tenant), "t_jpyg8359m89");
    ASSERT_STREQ(show_instance_statement->limit->offset->get_value().c_str(), "2");
    ASSERT_EQ(show_instance_statement->limit->limit, nullptr);
  }

  // case 5
  {
    std::string sql = "SHOW BINLOG INSTANCES FROM ip '10.253.251.10' LIMIT ALL;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_BINLOG_INSTANCE);
    auto* show_instance_statement = (hsql::ShowBinlogInstanceStatement*)result1.getStatement(0);
    ASSERT_EQ(show_instance_statement->history, false);
    ASSERT_EQ(show_instance_statement->mode, hsql::ShowInstanceMode::IP);
    ASSERT_STREQ(show_instance_statement->ip, "10.253.251.10");
    ASSERT_EQ(show_instance_statement->limit->offset, nullptr);
    ASSERT_EQ(show_instance_statement->limit->limit, nullptr);
  }

  // case 6
  {
    std::string sql = "SHOW BINLOG INSTANCES FROM ZONE 'hangzhou-a' limit 3;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_BINLOG_INSTANCE);
    auto* show_instance_statement = (hsql::ShowBinlogInstanceStatement*)result1.getStatement(0);
    ASSERT_EQ(show_instance_statement->history, false);
    ASSERT_EQ(show_instance_statement->mode, hsql::ShowInstanceMode::ZONE);
    ASSERT_STREQ(show_instance_statement->zone, "hangzhou-a");
    ASSERT_STREQ(show_instance_statement->limit->limit->get_value().c_str(), "3");
    ASSERT_EQ(show_instance_statement->limit->offset, nullptr);
  }

  // case 7
  {
    std::string sql = "SHOW BINLOG INSTANCES FROM region 'hangzhou' offset 2;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_BINLOG_INSTANCE);
    auto* show_instance_statement = (hsql::ShowBinlogInstanceStatement*)result1.getStatement(0);
    ASSERT_EQ(show_instance_statement->history, false);
    ASSERT_EQ(show_instance_statement->mode, hsql::ShowInstanceMode::REGION);
    ASSERT_STREQ(show_instance_statement->region, "hangzhou");
    ASSERT_STREQ(show_instance_statement->limit->offset->get_value().c_str(), "2");
    ASSERT_EQ(show_instance_statement->limit->limit, nullptr);
  }

  // case 8
  {
    std::string sql = "SHOW BINLOG INSTANCES FROM group 'g1' limit 3 offset 2;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_BINLOG_INSTANCE);
    auto* show_instance_statement = (hsql::ShowBinlogInstanceStatement*)result1.getStatement(0);
    ASSERT_EQ(show_instance_statement->history, false);
    ASSERT_EQ(show_instance_statement->mode, hsql::ShowInstanceMode::GROUP);
    ASSERT_EQ(std::string(show_instance_statement->group), "g1");
    ASSERT_STREQ(show_instance_statement->limit->offset->get_value().c_str(), "2");
    ASSERT_STREQ(show_instance_statement->limit->limit->get_value().c_str(), "3");
  }

  // case 9
  {
    std::string sql = "SHOW HISTORY BINLOG INSTANCES limit 2, 3;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_BINLOG_INSTANCE);
    auto* show_instance_statement = (hsql::ShowBinlogInstanceStatement*)result1.getStatement(0);
    ASSERT_EQ(show_instance_statement->history, true);
    ASSERT_EQ(show_instance_statement->mode, hsql::ShowInstanceMode::INSTANCE);
    ASSERT_EQ(nullptr, show_instance_statement->instance_names);
    ASSERT_STREQ(show_instance_statement->limit->offset->get_value().c_str(), "2");
    ASSERT_STREQ(show_instance_statement->limit->limit->get_value().c_str(), "3");
  }
}

TEST(SQLParser, switch_master)
{
  // case 1
  {
    std::string sql = "SWITCH MASTER INSTANCE to `aa` FOR TENANT `cluster`.`tenant`";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SWITCH_MASTER);
    auto* switch_master_statement = (hsql::SwitchMasterInstanceStatement*)result1.getStatement(0);
    ASSERT_EQ(switch_master_statement->full, false);
    ASSERT_STREQ(switch_master_statement->instance_name, "aa");
    ASSERT_STREQ(switch_master_statement->tenant_name->cluster, "cluster");
    ASSERT_STREQ(switch_master_statement->tenant_name->tenant, "tenant");
  }

  // case 2
  {
    std::string sql = "SWITCH MASTER INSTANCE to `aa` FOR TENANT `cluster`.`tenant` FULL";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SWITCH_MASTER);
    auto* switch_master_statement = (hsql::SwitchMasterInstanceStatement*)result1.getStatement(0);
    ASSERT_EQ(switch_master_statement->full, true);
    ASSERT_STREQ(switch_master_statement->instance_name, "aa");
    ASSERT_STREQ(switch_master_statement->tenant_name->cluster, "cluster");
    ASSERT_STREQ(switch_master_statement->tenant_name->tenant, "tenant");
  }
}

TEST(SQLParser, show_processlist)
{
  // case 1
  {
    std::string sql = "SHOW PROCESSLIST";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_PROCESS_LIST);
    auto* show_processlist_statement = (hsql::ShowProcessListStatement*)result1.getStatement(0);
    ASSERT_EQ(show_processlist_statement->full, false);
  }

  // case 2
  {
    std::string sql = "SHOW PROCESSLIST FOR INSTANCE `cluster$tenant$1`";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_PROCESS_LIST);
    auto* show_processlist_statement = (hsql::ShowProcessListStatement*)result1.getStatement(0);
    ASSERT_EQ(show_processlist_statement->full, false);
    ASSERT_STREQ(show_processlist_statement->instance_name, "cluster$tenant$1");
  }

  // case 3
  {
    std::string sql = "SHOW FULL PROCESSLIST";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_PROCESS_LIST);
    auto* show_processlist_statement = (hsql::ShowProcessListStatement*)result1.getStatement(0);
    ASSERT_EQ(show_processlist_statement->full, true);
  }

  // case 4
  {
    std::string sql = "SHOW FULL PROCESSLIST FOR INSTANCE `cluster$tenant$1`";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_PROCESS_LIST);
    auto* show_processlist_statement = (hsql::ShowProcessListStatement*)result1.getStatement(0);
    ASSERT_EQ(show_processlist_statement->full, true);
    ASSERT_STREQ(show_processlist_statement->instance_name, "cluster$tenant$1");
  }
}

TEST(SQLParser, show_dumplist)
{
  // case 1
  {
    std::string sql = "SHOW BINLOG DUMPLIST";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_DUMP_LIST);
    auto* show_dumplist_statement = (hsql::ShowDumpListStatement*)result1.getStatement(0);
    ASSERT_EQ(show_dumplist_statement->full, false);
  }

  // case 2
  {
    std::string sql = "SHOW BINLOG DUMPLIST FOR INSTANCE `cluster$tenant$1`";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_DUMP_LIST);
    auto* show_dumplist_statement = (hsql::ShowDumpListStatement*)result1.getStatement(0);
    ASSERT_EQ(show_dumplist_statement->full, false);
    ASSERT_STREQ(show_dumplist_statement->instance_name, "cluster$tenant$1");
  }

  // case 3
  {
    std::string sql = "SHOW FULL BINLOG DUMPLIST";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_DUMP_LIST);
    auto* show_dumplist_statement = (hsql::ShowDumpListStatement*)result1.getStatement(0);
    ASSERT_EQ(show_dumplist_statement->full, true);
  }

  // case 4
  {
    std::string sql = "SHOW FULL BINLOG DUMPLIST FOR INSTANCE `cluster$tenant$1`";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_SHOW_DUMP_LIST);
    auto* show_dumplist_statement = (hsql::ShowDumpListStatement*)result1.getStatement(0);
    ASSERT_EQ(show_dumplist_statement->full, true);
    ASSERT_STREQ(show_dumplist_statement->instance_name, "cluster$tenant$1");
  }
}

TEST(SQLParser, kill)
{
  // case 1
  {
    std::string sql = "KILL 3901628 FOR INSTANCE `cluster$tenant$1`;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_KILL);
    auto* kill_statement = (hsql::KillStatement*)result1.getStatement(0);
    ASSERT_EQ(kill_statement->only_kill_query, false);
    ASSERT_STREQ(kill_statement->process_id->get_value().c_str(), "3901628");
  }

  // case 2
  {
    std::string sql = "KILL 3901628 FOR INSTANCE `cluster$tenant$1`;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_KILL);
    auto* kill_statement = (hsql::KillStatement*)result1.getStatement(0);
    ASSERT_EQ(kill_statement->only_kill_query, false);
    ASSERT_STREQ(kill_statement->process_id->get_value().c_str(), "3901628");
    ASSERT_STREQ(kill_statement->instance_name, "cluster$tenant$1");
  }

  // case 3
  {
    std::string sql = "KILL CONNECTION 3901628;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_KILL);
    auto* kill_statement = (hsql::KillStatement*)result1.getStatement(0);
    ASSERT_EQ(kill_statement->only_kill_query, false);
    ASSERT_STREQ(kill_statement->process_id->get_value().c_str(), "3901628");
  }

  // case 4
  {
    std::string sql = "KILL CONNECTION 3901628 FOR INSTANCE `cluster$tenant$1`;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_KILL);
    auto* kill_statement = (hsql::KillStatement*)result1.getStatement(0);
    ASSERT_EQ(kill_statement->only_kill_query, false);
    ASSERT_STREQ(kill_statement->process_id->get_value().c_str(), "3901628");
    ASSERT_STREQ(kill_statement->instance_name, "cluster$tenant$1");
  }

  // case 5
  {
    std::string sql = "KILL QUERY 3901628;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_KILL);
    auto* kill_statement = (hsql::KillStatement*)result1.getStatement(0);
    ASSERT_EQ(kill_statement->only_kill_query, true);
    ASSERT_STREQ(kill_statement->process_id->get_value().c_str(), "3901628");
  }

  // case 6
  {
    std::string sql = "KILL QUERY 3901628 FOR INSTANCE `cluster$tenant$1`;";

    hsql::SQLParserResult result1;
    int ret = ObSqlParser::parse(sql, result1);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result1.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_KILL);
    auto* kill_statement = (hsql::KillStatement*)result1.getStatement(0);
    ASSERT_EQ(kill_statement->only_kill_query, true);
    ASSERT_STREQ(kill_statement->process_id->get_value().c_str(), "3901628");
    ASSERT_STREQ(kill_statement->instance_name, "cluster$tenant$1");
  }
}

TEST(SQLParser, report)
{
  // case 1
  {
    std::string sql = "REPORT;";
    hsql::SQLParserResult result1;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(sql, result1));
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_REPORT);
  }

  // case 2
  {
    std::string sql = "REPORT 0;";
    hsql::SQLParserResult result1;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(sql, result1));
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_REPORT);
    auto* report_statement = (hsql::ReportStatement*)result1.getStatement(0);
    ASSERT_STREQ(report_statement->timestamp->get_value().c_str(), "0");
  }

  // case 3
  {
    std::string sql = "REPORT 1721224673;";
    hsql::SQLParserResult result1;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(sql, result1));
    ASSERT_EQ(result1.getStatement(0)->type(), hsql::COM_REPORT);
    auto* report_statement = (hsql::ReportStatement*)result1.getStatement(0);
    ASSERT_STREQ(report_statement->timestamp->get_value().c_str(), "1721224673");
  }
}

TEST(SQLParser, select)
{
  std::string query = "SELECT * FROM test;";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
}

TEST(SQLParser, set)
{
  {
    std::string query = "set master_binlog_checksum=100000;";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  }
  {
    std::string query = "set BINLOG_EXPIRE_LOGS_SIZE=536870912;";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  }
  {
    std::string query = "set ROOTSERVER_LIST='127.0.0.1:11:12';";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  }
  {
    std::string query = "set START_TIMESTAMP=18732477;";
    hsql::SQLParserResult result;
    int ret = ObSqlParser::parse(query, result);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result.errorMsg());
    }
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result.getStatement(0)->type(), hsql::COM_SET);
    auto* set_statement = (hsql::SetStatement*)result.getStatement(0);
    ASSERT_STREQ(set_statement->sets->front()->column, "start_timestamp");
  }
}

TEST(SQLParser, set_global)
{
  {
    std::smatch sm;
    std::string query = "set global master_binlog_checksum=100000;";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  }
}

TEST(SQLParser, set_session)
{
  std::smatch sm;
  std::string query = "set  session master_binlog_checksum=100000;";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
}

TEST(SQLParser, set_local)
{
  std::string query = "set local master_binlog_checksum=100000;";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
}

TEST(SQLParser, set_session_)
{
  std::string query = "set @master_binlog_checksum='CRC32',@slave_uuid=777;";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
}

TEST(SQLParser, set_password)
{
  {
    std::string sql = "set password for sysadmin = password('new_password');";
    hsql::SQLParserResult result;
    int ret = ObSqlParser::parse(sql, result);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result.getStatement(0)->type(), hsql::COM_SET_PASSWORD);
    auto* set_password_statement = (hsql::SetPasswordStatement*)result.getStatement(0);
    ASSERT_STREQ(set_password_statement->user, "sysadmin");
    ASSERT_STREQ(set_password_statement->password, "new_password");
  }
  {
    std::string sql = "set password for sysadmin = 'new_password';";
    hsql::SQLParserResult result;
    int ret = ObSqlParser::parse(sql, result);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result.getStatement(0)->type(), hsql::COM_SET_PASSWORD);
    auto* set_password_statement = (hsql::SetPasswordStatement*)result.getStatement(0);
    ASSERT_STREQ(set_password_statement->user, "sysadmin");
    ASSERT_STREQ(set_password_statement->password, "new_password");
  }
  {
    std::string sql = "set password for sysadmin = '';";
    hsql::SQLParserResult result;
    int ret = ObSqlParser::parse(sql, result);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result.errorMsg());
    }

    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result.getStatement(0)->type(), hsql::COM_SET_PASSWORD);
    auto* set_password_statement = (hsql::SetPasswordStatement*)result.getStatement(0);
    ASSERT_STREQ(set_password_statement->user, "sysadmin");
    ASSERT_STREQ(set_password_statement->password, "");
  }
}

TEST(SQLParser, select_sum)
{
  std::string query = "select sum(1);";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
}

TEST(SQLParser, select_gtid_subset)
{
  std::string query = "SELECT GTID_SUBSET('1-1-1,2-2-2,3-3-3', '1-1-1,2-2-2');";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
}

TEST(SQLParser, select_gtid_subset_lower)
{
  std::string query = "SELECT gtid_subset('1-1-1,2-2-2,3-3-3', '1-1-1,2-2-2');";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
}

TEST(SQLParser, select_gtid_subtract)
{
  {
    std::string query = "select "
                        "gtid_subtract('d9dd8510-1321-11ee-a9a7-7cd30abc99b4:1-51774,d9dd8510-1321-11ee-a9a7-"
                        "7cd302bc99b4:1-51774','d9dd8510-1321-11ee-a9a7-7cd30abc99b4:1-51775');";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    hsql::SelectStatement* p_statement = (hsql::SelectStatement*)result.getStatement(0);
    auto gtid_sub = p_statement->selectList->at(0)->exprList->at(0)->getName();
    auto gtid_target = p_statement->selectList->at(0)->exprList->at(1)->getName();
    std::string sql = "";
    BinlogFunction::gtid_subtract(gtid_sub, gtid_target, sql);
    ASSERT_STREQ(sql.c_str(), "d9dd8510-1321-11ee-a9a7-7cd302bc99b4:1-51774");
  }
  {
    std::string query = "select "
                        "gtid_subtract('d9dd8510-1321-11ee-a9a7-7cd30abc99b4:1-999994,d9dd8510-1321-11ee-a9a7-"
                        "7cd302bc99b4:1-51774','d9dd8510-1321-11ee-a9a7-7cd30abc99b4:1-51775');";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    hsql::SelectStatement* p_statement = (hsql::SelectStatement*)result.getStatement(0);
    auto gtid_sub = p_statement->selectList->at(0)->exprList->at(0)->getName();
    auto gtid_target = p_statement->selectList->at(0)->exprList->at(1)->getName();
    std::string sql = "";
    BinlogFunction::gtid_subtract(gtid_sub, gtid_target, sql);
    ASSERT_STREQ(
        sql.c_str(), "d9dd8510-1321-11ee-a9a7-7cd302bc99b4:1-51774,d9dd8510-1321-11ee-a9a7-7cd30abc99b4:51776-999994");
  }
}

TEST(SQLParser, select_master_binlog_checksum)
{
  std::string query = "SELECT @master_binlog_checksum;";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  ASSERT_EQ(hsql::COM_SELECT, result.getStatement(0)->type());
  auto* statement = (hsql::SelectStatement*)result.getStatement(0);
  hsql::Expr* expr = statement->selectList->at(0);
  ASSERT_EQ(hsql::kExprVar, expr->type);
  ASSERT_STREQ("master_binlog_checksum", expr->getName());
  ASSERT_EQ(hsql::kUser, expr->var_type);
  ASSERT_EQ(hsql::Session, expr->var_level);
}

TEST(SQLParser, set_master_heartbeat_period)
{
  std::string query = "SET @master_heartbeat_period=10";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  ASSERT_EQ(hsql::COM_SET, result.getStatement(0)->type());
  auto* statement = (hsql::SetStatement*)result.getStatement(0);
  std::vector<hsql::SetClause*>* set_clause = statement->sets;
  ASSERT_STREQ("master_heartbeat_period", set_clause->at(0)->column);
  ASSERT_EQ(hsql::kUser, set_clause->at(0)->var_type);
  ASSERT_EQ(hsql::Session, set_clause->at(0)->type);
}

TEST(SQLParser, select_global)
{
  std::string query = "select @@system_variable_name;";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  ASSERT_EQ(hsql::COM_SELECT, result.getStatement(0)->type());
  auto* statement = (hsql::SelectStatement*)result.getStatement(0);
  hsql::Expr* expr = statement->selectList->at(0);
  ASSERT_EQ(hsql::kExprVar, expr->type);
  ASSERT_STREQ("system_variable_name", expr->getName());
  ASSERT_EQ(hsql::kSys, expr->var_type);
  ASSERT_EQ(hsql::Global, expr->var_level);
}

TEST(SQLParser, show_var)
{
  {
    std::string query = "SHOW VARIABLES LIKE 'max_connections';";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  }
  {
    std::string query = "SHOW VARIABLES FOR INSTANCE `aa`;";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    auto* statement = (hsql::ShowStatement*)result.getStatement(0);
    ASSERT_STREQ(statement->instance_name, "aa");
  }
  {
    std::string query = "SHOW GLOBAL VARIABLES FOR INSTANCE `aa`;";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    auto* statement = (hsql::ShowStatement*)result.getStatement(0);
    ASSERT_STREQ(statement->instance_name, "aa");
  }
  {
    std::string query = "SHOW LOCAL VARIABLES FOR INSTANCE `aa`;";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    auto* statement = (hsql::ShowStatement*)result.getStatement(0);
    ASSERT_STREQ(statement->instance_name, "aa");
  }
  {
    std::string query = "SHOW SESSION VARIABLES FOR INSTANCE `aa`;";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    auto* statement = (hsql::ShowStatement*)result.getStatement(0);
    ASSERT_STREQ(statement->instance_name, "aa");
  }
  {
    std::string query = "SHOW VARIABLES LIKE 'max_connections' FOR INSTANCE `aa`;";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    auto* statement = (hsql::ShowStatement*)result.getStatement(0);
    ASSERT_STREQ(statement->instance_name, "aa");
  }
  {
    std::string query = "SHOW GLOBAL VARIABLES LIKE 'max_connections' FOR INSTANCE `aa`;";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    auto* statement = (hsql::ShowStatement*)result.getStatement(0);
    ASSERT_STREQ(statement->instance_name, "aa");
  }
  {
    std::string query = "SHOW LOCAL VARIABLES LIKE 'max_connections' FOR INSTANCE `aa`;";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    auto* statement = (hsql::ShowStatement*)result.getStatement(0);
    ASSERT_STREQ(statement->instance_name, "aa");
  }
  {
    std::string query = "SHOW SESSION VARIABLES LIKE 'max_connections' FOR INSTANCE `aa`;";
    hsql::SQLParserResult result;
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    auto* statement = (hsql::ShowStatement*)result.getStatement(0);
    ASSERT_STREQ(statement->instance_name, "aa");
  }
}

TEST(SQLParser, show_gtid_executed)
{
  std::string query = "show global variables like 'gtid_executed'";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  hsql::ShowStatement* show_statement = (hsql::ShowStatement*)result.getStatement(0);
  ASSERT_EQ(hsql::Global, show_statement->var_type);
}

TEST(SQLParser, select_limit)
{
  std::string query = "select @@version_comment limit 1;";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
}

TEST(SQLParser, drop_binlog)
{
  {
    std::string query = "DROP BINLOG IF EXISTS FOR TENANT `ob4prf098hngao`.`t4prgngfyd4lc`";
    hsql::SQLParserResult result;
    int ret = ObSqlParser::parse(query, result);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result.errorMsg());
    }
    ASSERT_EQ(OMS_OK, ret);
    ASSERT_EQ(result.getStatement(0)->type(), hsql::COM_DROP_BINLOG);
  }
  {
    std::string query = "DROP BINLOG IF EXISTS FOR TENANT `ob4prf098hngao`.`t4prgngfyd4lc` now";
    hsql::SQLParserResult result;
    int ret = ObSqlParser::parse(query, result);
    if (OMS_OK != ret) {
      OMS_INFO("parse result: {}", result.errorMsg());
    }
    ASSERT_EQ(OMS_OK, ret);
    auto* drop_statement = (hsql::DropBinlogStatement*)result.getStatement(0);
    ASSERT_STREQ(drop_statement->defer_execution_sec->get_value().c_str(), "0");
  }
}

TEST(SQLParser, session)
{
  std::string query =
      "SET @@ob_enable_transmission_checksum = 1, @@ob_trace_info = 'client_ip=172.17.0.15', @@wait_timeout = 9999999, "
      "@@net_write_timeout = 7200, @@net_read_timeout = 7200, @@character_set_client = 63, @@character_set_results = "
      "63, @@character_set_connection = 63, @@collation_connection = 63, @master_binlog_checksum = 'CRC32', "
      "@slave_uuid = '3d241ade-167f-11ee-a9a7-7cd30abc99b4', @mariadb_slave_capability = X'34', "
      "@master_heartbeat_period = 15000000000;";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  auto* statement = (hsql::SetStatement*)result.getStatement(0);
  std::vector<hsql::SetClause*>* set_clause = statement->sets;
  ASSERT_STREQ("ob_enable_transmission_checksum", set_clause->at(0)->column);
  ASSERT_STREQ("ob_trace_info", set_clause->at(1)->column);
  ASSERT_STREQ("wait_timeout", set_clause->at(2)->column);
  ASSERT_STREQ("net_write_timeout", set_clause->at(3)->column);
  ASSERT_STREQ("net_read_timeout", set_clause->at(4)->column);
  ASSERT_STREQ("character_set_client", set_clause->at(5)->column);
  ASSERT_STREQ("character_set_results", set_clause->at(6)->column);
  ASSERT_STREQ("mariadb_slave_capability", set_clause->at(11)->column);
  ASSERT_STREQ("4", set_clause->at(11)->value->get_value().c_str());
}

TEST(SQLParser, session_show_ddl)
{
  std::string query =
      "SET @@character_set_database = 28, @@character_set_server = 28, @@collation_database = 28, "
      "@@collation_server = 28, @@ob_trx_timeout = 10000000000000, @@ob_enable_transmission_checksum = "
      "1, @@ob_trx_idle_timeout = 10000000000, @@_show_ddl_in_compat_mode = 1, @@wait_timeout = "
      "9999999, @@net_write_timeout = 7200, @@net_read_timeout = 7200, @@character_set_client = 63, "
      "@@character_set_results = 63, @@character_set_connection = 63, @@collation_connection = 63, "
      "@@ob_query_timeout = 1000000000000, @_min_cluster_version = '4.1.0.1', @master_binlog_checksum "
      "= X'4352433332', @slave_uuid = '806f3835-209d-11ee-aa36-00163e0c9be7', "
      "@mariadb_slave_capability = X'34', @master_heartbeat_period = 15000000000, @master_binlog_checksum "
      "= 0x4352433332;";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  auto* statement = (hsql::SetStatement*)result.getStatement(0);
  std::vector<hsql::SetClause*>* set_clause = statement->sets;
  ASSERT_STREQ("master_binlog_checksum", set_clause->at(17)->column);
  ASSERT_STREQ("CRC32", set_clause->at(17)->value->get_value().c_str());
  ASSERT_STREQ("4", set_clause->at(19)->value->get_value().c_str());
  ASSERT_STREQ("CRC32", set_clause->at(21)->value->get_value().c_str());
}

TEST(SQLParser, session_hex)
{
  {
    std::string query = "X'4352433332'";
    size_t len = strlen(query.c_str()) - 2;
    size_t end_pos = strlen(query.c_str());
    if ('\'' == query[strlen(query.c_str()) - 1]) {
      // Values written using X'val' notation
      --len;
      --end_pos;
    }
    query = hsql::substr(query.c_str(), 2, end_pos);
    std::string result = strdup(hsql::hex2bin(query.c_str(), strlen(query.c_str())));
    ASSERT_STREQ("CRC32", result.c_str());
  }
  {
    std::string query = "0x4352433332";
    size_t len = strlen(query.c_str()) - 2;
    size_t end_pos = strlen(query.c_str());
    if ('\'' == query[strlen(query.c_str()) - 1]) {
      // Values written using X'val' notation
      --len;
      --end_pos;
    }
    query = hsql::substr(query.c_str(), 2, end_pos);
    std::string result = strdup(hsql::hex2bin(query.c_str(), strlen(query.c_str())));
    ASSERT_STREQ("CRC32", result.c_str());
  }
}

TEST(SQLParser, show_binlog_events)
{
  std::string query = "show binlog events limit 1";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  auto* statement = result.getStatement(0);
  auto* p_statement = (hsql::ShowBinlogEventsStatement*)statement;
  std::string binlog_file;
  std::string start_pos;
  std::string offset_str;
  std::string limit_str;

  if (p_statement->binlog_file != nullptr) {
    binlog_file = p_statement->binlog_file->get_value();
  }

  if (p_statement->start_pos != nullptr) {
    start_pos = p_statement->start_pos->get_value();
  }

  if (p_statement->limit != nullptr) {
    if (p_statement->limit->offset != nullptr) {
      offset_str = p_statement->limit->offset->get_value();
    }

    if (p_statement->limit->limit != nullptr) {
      limit_str = p_statement->limit->limit->get_value();
    }
  }

  ASSERT_EQ(1, atoll(limit_str.c_str()));
}

TEST(SQLParser, show_binlog_events_limit)
{
  std::string query = "show binlog events limit 1;";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  auto* statement = result.getStatement(0);
  auto* p_statement = (hsql::ShowBinlogEventsStatement*)statement;
  std::string binlog_file;
  std::string start_pos;
  std::string offset_str;
  std::string limit_str;

  if (p_statement->binlog_file != nullptr) {
    binlog_file = p_statement->binlog_file->get_value();
  }

  if (p_statement->start_pos != nullptr) {
    start_pos = p_statement->start_pos->get_value();
  }

  if (p_statement->limit != nullptr) {
    if (p_statement->limit->offset != nullptr) {
      offset_str = p_statement->limit->offset->get_value();
    }

    if (p_statement->limit->limit != nullptr) {
      limit_str = p_statement->limit->limit->get_value();
    }
  }

  ASSERT_EQ(1, atoll(limit_str.c_str()));
}

TEST(SQLParser, show_binlog_events_in_file_limit)
{
  std::string query = "show binlog events in 'binlog.000001' limit 1;";
  hsql::SQLParserResult result;
  ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
  auto* statement = result.getStatement(0);
  auto* p_statement = (hsql::ShowBinlogEventsStatement*)statement;
  std::string binlog_file;
  std::string start_pos;
  std::string offset_str;
  std::string limit_str;

  if (p_statement->binlog_file != nullptr) {
    binlog_file = p_statement->binlog_file->get_value();
  }

  if (p_statement->start_pos != nullptr) {
    start_pos = p_statement->start_pos->get_value();
  }

  if (p_statement->limit != nullptr) {
    if (p_statement->limit->offset != nullptr) {
      offset_str = p_statement->limit->offset->get_value();
    }

    if (p_statement->limit->limit != nullptr) {
      limit_str = p_statement->limit->limit->get_value();
    }
  }

  ASSERT_EQ(1, atoll(limit_str.c_str()));
  ASSERT_EQ("binlog.000001", binlog_file);
}

TEST(SQLParser, show_nodes)
{
  // SHOW NODES
  {
    hsql::SQLParserResult result;
    std::string query = "SHOW NODES";
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    ASSERT_EQ(hsql::COM_SHOW_NODES, result.getStatement(0)->type());
    auto* p_statement = (hsql::ShowNodesStatement*)result.getStatement(0);
    ASSERT_EQ(p_statement->option, hsql::ShowNodeOption::NODE_ALL);
  }

  // SHOW NODES FROM IP '127.0.0.1'
  {
    hsql::SQLParserResult result;
    std::string query = "SHOW NODES FROM IP '127.0.0.1'";
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    ASSERT_EQ(hsql::COM_SHOW_NODES, result.getStatement(0)->type());
    auto* p_statement = (hsql::ShowNodesStatement*)result.getStatement(0);
    ASSERT_EQ(p_statement->option, hsql::ShowNodeOption::NODE_IP);
    ASSERT_STREQ(p_statement->ip, "127.0.0.1");
  }

  // SHOW NODES FROM REGION '127.0.0.1'
  {
    hsql::SQLParserResult result;
    std::string query = "SHOW NODES FROM REGION 'cn-hangzhou'";
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    ASSERT_EQ(hsql::COM_SHOW_NODES, result.getStatement(0)->type());
    auto* p_statement = (hsql::ShowNodesStatement*)result.getStatement(0);
    ASSERT_EQ(p_statement->option, hsql::ShowNodeOption::NODE_REGION);
    ASSERT_STREQ(p_statement->region, "cn-hangzhou");
  }

  // SHOW NODES FROM GROUP 'xxx'
  {
    hsql::SQLParserResult result;
    std::string query = "SHOW NODES FROM GROUP 'xxx'";
    ASSERT_EQ(OMS_OK, ObSqlParser::parse(query, result));
    ASSERT_EQ(hsql::COM_SHOW_NODES, result.getStatement(0)->type());
    auto* p_statement = (hsql::ShowNodesStatement*)result.getStatement(0);
    ASSERT_EQ(p_statement->option, hsql::ShowNodeOption::NODE_GROUP);
    ASSERT_STREQ(p_statement->group, "xxx");
  }
}
