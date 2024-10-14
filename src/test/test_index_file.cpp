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
#include "common.h"
#include "binlog/binlog_index.h"
#include "binlog/common_util.h"
#include "thread.h"
#include "log.h"

using namespace oceanbase::logproxy;
using namespace oceanbase::binlog;
namespace fs = std::filesystem;

BinlogIndexManager g_index_manager("mysql-bin.index");
TEST(IndexFile, add_index)
{
  BinlogIndexRecord record;
  record.set_index(1);
  record.set_file_name(fs::current_path().string() + "/" + "mysql-bin.1");
  record.set_position(12543);
  std::string path(fs::current_path().string() + "/");
  BinlogIndexManager index_manager(path + BINLOG_INDEX_NAME);
  index_manager.add_index(record);

  record.set_index(2);
  record.set_file_name(fs::current_path().string() + "/" + "mysql-bin.2");
  record.set_position(72578406);
  index_manager.add_index(record);
  ASSERT_EQ(true, FsUtil::remove(path + BINLOG_INDEX_NAME, false));
}

TEST(IndexFile, release_vector)
{
  std::vector<BinlogIndexRecord*> vector_ptr;

  auto* val_1 = new BinlogIndexRecord();
  vector_ptr.emplace_back(val_1);

  release_vector(vector_ptr);
}

TEST(IndexFile, binlog_file_utils)
{
  {
    uint32_t index = 1;
    std::string file_name = oceanbase::binlog::CommonUtils::fill_binlog_file_name(index);
    ASSERT_EQ("mysql-bin.000001", file_name);
  }
  {
    uint64_t index = UINT64_MAX;
    std::string file_name = oceanbase::binlog::CommonUtils::fill_binlog_file_name(index);
    ASSERT_EQ("mysql-bin.18446744073709551615", file_name);
  }
  {
    uint64_t index = 999999;
    std::string file_name = oceanbase::binlog::CommonUtils::fill_binlog_file_name(index);
    ASSERT_EQ("mysql-bin.999999", file_name);
  }
  {
    uint64_t index = 65535;
    std::string file_name = oceanbase::binlog::CommonUtils::fill_binlog_file_name(index);
    ASSERT_EQ("mysql-bin.065535", file_name);
  }
}

class MockConverter : public Thread {
public:
  void run() override
  {
    int index_file_count = 0;
    while (index_file_count++ < 50) {
      BinlogIndexRecord index_record(CommonUtils::fill_binlog_file_name(index_file_count), index_file_count);
      g_index_manager.add_index(index_record);
      int update_time = 0;
      while (update_time++ < 10) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        index_record.set_position(update_time * 100);
        g_index_manager.update_index(index_record);
      }
    }
  }
};

class MockDumper : public Thread {
public:
  void run() override
  {
    std::string file = "mysql-bin.000001";
    uint64_t pos = 0;
    BinlogIndexRecord record;
    while (is_run()) {
      bool new_event_coming = g_index_manager.is_behind_current_pos(file, pos, 50 * 1000);
      wake_times = new_event_coming ? wake_times + 1 : wake_times;
      g_index_manager.get_memory_index(record);
      file = record.get_file_name();
      pos = record.get_position();
    }
    ASSERT_EQ("mysql-bin.000050", file);
    ASSERT_EQ(1000, pos);
  }

  int get_wake_times() const
  {
    return wake_times;
  }

private:
  int wake_times = 0;
};

class MockFetchBinlogs : public Thread {
public:
  void run() override
  {
    while (is_run()) {
      std::vector<BinlogIndexRecord*> index_records;
      g_index_manager.fetch_index_vector(index_records);
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      release_vector(index_records);
    }
  }
};

class MockPurgeBinlogs : public Thread {
public:
  void run() override
  {
    int binlog_file_count = 0;
    while (is_run()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      binlog_file_count++;
      if (binlog_file_count % 5 == 0) {
        std::string error_msg;
        std::vector<std::string> purge_binlog_files;
        g_index_manager.purge_binlog_index(
            "", CommonUtils::fill_binlog_file_name(binlog_file_count - 2), "", error_msg, purge_binlog_files);
      }
    }
  }
};

TEST(IndexFile, multi_thread_access)
{
  MockConverter converter;
  MockDumper dumper;
  MockFetchBinlogs fetch_binlogs;
  MockPurgeBinlogs purge_binlogs;

  purge_binlogs.start();
  fetch_binlogs.start();
  dumper.start();
  converter.start();

  converter.join();
  dumper.stop();
  fetch_binlogs.stop();
  purge_binlogs.stop();

  ASSERT_EQ(500, dumper.get_wake_times());
  FsUtil::remove("mysql-bin.index");
}
