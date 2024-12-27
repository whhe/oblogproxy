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

#include "env.h"
#include "fs_util.h"
#include "config.h"
#include "trace_log.h"
#include "binlog_converter.h"
#include "instance_socket_listener.h"

using namespace oceanbase::logproxy;
using namespace oceanbase::binlog;

int init_configs(const std::string& config_file);

int preprocess_breakpoint_binlog();

int main(int argc, char** argv)
{
  // 0. change and check work path
  std::string work_path(argv[1]);
  ::chdir(work_path.c_str());
  replace_spdlog_default_logger();

  int tcp_port = atoi(argv[3]);
  InstanceSocketListener socket_listener(tcp_port);
  if (OMS_OK != socket_listener.try_lock()) {
    OMS_ERROR("!!! Exiting binlog instance process: {}, due to work path [{}] is locked by other process.",
        getpid(),
        work_path);
    ::exit(-1);
  }

  // 1. init
  std::string config_file(argv[2]);
  if (OMS_OK != init_configs(config_file)) {
    OMS_ERROR("!!! Exiting binlog instance process: {}, due to failed to init configs.", getpid());
    ::exit(-1);
  }

  const char* child_process_name = argv[0];
  Config& conf = Config::instance();
  conf.process_name_address.set((uint64_t)child_process_name);

  FsUtil::mkdir("./log");
  FsUtil::mkdir("./data");

  // !!! Before init logger, logs will be printed to the default logger (i.e., log/init.log) !!!
  init_log(child_process_name);
  if (conf.verbose_record_read.val()) {
    TraceLog::init(conf.log_max_file_size_mb.val(), conf.log_retention_h.val());
  }

  OMS_INFO("Begin to start binlog instance with given params, work path: {}, config file: {}, tcp port: {}",
      work_path,
      config_file,
      tcp_port);
  uint32_t sys_var_nof_work_threads = s_config.binlog_nof_work_threads.val();
  uint32_t binlog_obi_column_work_threads = s_config.binlog_obi_column_work_threads.val();
  uint32_t obi_purge_binlog_threads = s_config.binlog_purge_binlog_threads.val();
  if (OMS_OK != instance_env_init(sys_var_nof_work_threads, binlog_obi_column_work_threads, obi_purge_binlog_threads)) {
    OMS_ERROR("!!! Exit binlog instance [{}] due to env init failure.", s_meta.instance_name());
    ::exit(-1);
  }

  if (OMS_OK != s_meta.config_server_options()) {
    OMS_ERROR("!!! Exit binlog instance [{}] due to config sever options failure.", s_meta.instance_name());
    ::exit(-1);
  }

  // Initialize the local gtid mapping relationship
  g_gtid_manager->init_gtid_seq();

  // 2. preprocess binlog
  if (OMS_OK != preprocess_breakpoint_binlog()) {
    OMS_ERROR("!!! Exit binlog instance [{}] due to failed to preprocess breakpoint binlog", s_meta.instance_name());
    ::exit(-1);
  }

  // init gtid_seq、gtid_purged、gtid_executed
  if (OMS_OK != g_gtid_manager->init()) {
    OMS_ERROR("!!! Exit binlog instance [{}] due to failed to init gtid related variables.", s_meta.instance_name());
    ::exit(-1);
  }

  // 3. enable tcp & uds listening
  socket_listener.start();

  // 4. start binlog converter thread asynchronously
  BinlogConverter::instance().start();

  socket_listener.join();
  OMS_ERROR("Instance socket listener has exited, and stop binlog converter manually.");
  if (BinlogConverter::instance().is_run()) {
    BinlogConverter::instance().stop_converter();
    BinlogConverter::instance().join_converter();
  }
  OMS_ERROR("!!! Exit binlog instance: {}", s_meta.instance_name());

  instance_env_deInit();
  return 0;
}

int init_configs(const std::string& config_file)
{
  rapidjson::Document doc;
  if (OMS_OK != load_configs(config_file, doc)) {
    return OMS_FAILED;
  }

  if ((!doc.HasMember(CONFIG) || !doc[CONFIG].IsObject() || doc[CONFIG].IsNull()) ||
      (!doc.HasMember(INSTANCE_META) || !doc[INSTANCE_META].IsObject() || doc[INSTANCE_META].IsNull())) {
    OMS_ERROR("Invalid config json format: {}", config_file);
    return OMS_FAILED;
  }

  // deserialize Config
  if (OMS_OK != s_config.from_json(doc[CONFIG])) {
    OMS_ERROR("Failed to parse instance config.");
    return OMS_FAILED;
  }
  OMS_INFO("Parsed {}: \n{}", CONFIG, s_config.to_string(true, true));

  // deserialize InstanceMate
  if (OMS_OK != s_meta.deserialize_from_json_value(doc[INSTANCE_META])) {
    OMS_ERROR("Failed to parse instance meta.");
    return OMS_FAILED;
  }
  OMS_INFO("Parsed {}: \n{}", INSTANCE_META, s_meta.serialize_to_json());

  OMS_INFO("Success to init instance meta of binlog instance from json file: {}", config_file);
  return OMS_OK;
}

int preprocess_breakpoint_binlog()
{
  BinlogIndexRecord index_record;
  g_index_manager->get_latest_index(index_record);
  if (index_record.get_file_name().empty()) {
    return OMS_OK;
  }

  BinlogTrxOverview binlog_trx;
  binlog_trx.trx_specify_truncated_gtid = index_record.get_before_mapping().second;
  if (OMS_OK != seek_and_verify_complete_trx(index_record.get_file_name(), binlog_trx)) {
    OMS_ERROR("Failed to open or verify breakpoint binlog: {}", index_record.get_file_name());
    return OMS_FAILED;
  }
  binlog_trx.log_detail();
  if (s_config.binlog_recover_backup.val()) {
    g_index_manager->backup_binlog(index_record);
  }

  // Scenario 1: incomplete binlog file
  if (!binlog_trx.has_previous_gtids_event) {
    if (OMS_OK != g_index_manager->remove_binlog(index_record)) {
      OMS_ERROR(
          "Failed to delete the last binlog file [{}] without previous_gtids event", index_record.get_file_name());
      return OMS_FAILED;
    }
    OMS_INFO("Delete incomplete breakpoint binlog file: {}", index_record.get_file_name());
    return OMS_OK;
  }

  // Scenario 2: all are complete transactions,Only when the transaction mapping relationship completely exists in the
  // index file, the current binlog is considered to be legal.

  // Scenario 2.1: Determine whether the current complete transaction has a mapping relationship in the index file
  // 1. If there is a mapping relationship, it means that the current binlog and index files are complete, and they can
  // be truncated and returned.
  // 2. If there is no mapping relationship, it means that the current binlog is incomplete. You need to find the
  // mapping relationship of the latest complete transaction and then truncate the transaction.

  if (binlog_trx.last_complete_trx_gtid <= index_record.get_current_mapping().second &&
      binlog_trx.last_complete_trx_gtid >= index_record.get_before_mapping().second) {
    if (binlog_trx.trx_need_truncated_gtid == 0) {
      // To compensate for the file size under update, it is possible that the index has not been updated during the
      // first initialization.
      uint64_t file_size = FsUtil::file_size(index_record.get_file_name());
      if (index_record.get_position() != file_size) {
        index_record.set_position(file_size);
        g_index_manager->update_index(index_record);
      }
      OMS_INFO("Breakpoint binlog file [{}] is complete, and the last transaction gtid: {}",
          index_record.get_file_name(),
          binlog_trx.last_complete_trx_gtid);
      return OMS_OK;
    }

    // Scenario 3: exist incomplete transaction
    OMS_INFO(
        "There exist incomplete transaction in breakpoint binlog [{}], transaction with gtid need truncated: {}, last "
        "complete transaction with gtid: {}",
        index_record.get_file_name(),
        binlog_trx.trx_need_truncated_gtid,
        binlog_trx.last_complete_trx_gtid);
    BinlogIndexRecord correct_index_record = index_record;
    uint64_t file_size = FsUtil::file_size(index_record.get_file_name());
    if (binlog_trx.last_complete_trx_end_pos < file_size) {
      error_code err;
      fs::resize_file(index_record.get_file_name(), binlog_trx.last_complete_trx_end_pos, err);
      if (err) {
        OMS_ERROR(
            "Failed to truncate incomplete transaction with gtid [{}] in breakpoint binlog file [{}] from [{}] to "
            "[{}], error: {}",
            binlog_trx.trx_need_truncated_gtid,
            index_record.get_file_name(),
            file_size,
            binlog_trx.last_complete_trx_end_pos,
            err.message());
        return OMS_FAILED;
      }
      OMS_INFO("Truncate incomplete transaction with gtid [{}] in breakpoint binlog file [{}] from [{}] to [{}]",
          binlog_trx.trx_need_truncated_gtid,
          index_record.get_file_name(),
          file_size,
          binlog_trx.last_complete_trx_end_pos);
    }

    // correct binlog index record
    if (binlog_trx.last_complete_trx_gtid > 0) {
      correct_index_record.set_position(binlog_trx.last_complete_trx_end_pos);
      correct_index_record.set_current_mapping(index_record.get_before_mapping());
    } else {
      correct_index_record.set_before_mapping(std::pair<std::string, uint64_t>("", 0));
    }

    g_index_manager->update_index(correct_index_record);
    OMS_INFO("Correct the index of the breakpoint binlog based last transaction with git [{}] from [{}] to [{}]",
        binlog_trx.last_complete_trx_gtid,
        index_record.to_string(),
        correct_index_record.to_string());

    return OMS_OK;

  } else {
    if (binlog_trx.last_complete_trx_gtid == 0 && binlog_trx.trx_need_truncated_gtid == 0) {
      OMS_INFO("Breakpoint binlog file [{}] is complete, and the last transaction gtid: {},{}",
          index_record.get_file_name(),
          index_record.get_current_mapping().first,
          index_record.get_current_mapping().second);
      return OMS_OK;
    }
    // The binlog file lags behind the index file
    if (binlog_trx.last_complete_trx_gtid < index_record.get_before_mapping().second) {
      // The mapping relationship does not exist, and the binlog file needs to be truncated or the index file mapping
      // relationship needs to be updated.

      // The scenario at this time is: there is a complete transaction in the current binlog file, but the gtid of the
      // transaction has no mapping relationship found in the index file. You need to find the mapping relationship of
      // the most recent transaction ID in the gtid mapping file, and then truncate the transaction.
      // 1. Find the mapping relationship of the latest transaction ID in the gtid mapping file
      // 2. Truncate the transaction
      GtidSeq gtid_seq;
      if (OMS_OK != g_gtid_manager->get_first_le_gtid_seq(binlog_trx.last_complete_trx_gtid, gtid_seq)) {
        OMS_ERROR("A gtid less than or equal to gtid:{} was not found in the gitd mapping file, and the corresponding "
                  "mapping relationship failed to be obtained.",
            binlog_trx.last_complete_trx_gtid);
        return OMS_FAILED;
      }
      OMS_INFO("First le gtid seq:{}", gtid_seq.serialize());

      BinlogTrxOverview trx_overview;
      trx_overview.trx_specify_truncated_gtid = gtid_seq.get_gtid_start();
      if (OMS_OK != seek_and_verify_complete_trx(index_record.get_file_name(), trx_overview)) {
        OMS_ERROR("Failed to open or verify breakpoint binlog: {}", index_record.get_file_name());
        return OMS_FAILED;
      }
      trx_overview.log_detail();

      // Determine whether the corresponding gtid data is found in the binlog file. If not, it means that there is no
      // corresponding gtid data in the binlog file. Just delete the entire binlog file. The previous binlog file must
      // be guaranteed to have the corresponding gtid mapping relationship. If it is found, it means that the
      // corresponding gtid data exists in the binlog file. The binlog needs to be truncated to the
      // trx_specify_truncated_end_pos position and the mapping relationship in the index file should be updated.
      if (trx_overview.trx_specify_truncated_end_pos == 0) {
        if (OMS_OK != g_index_manager->remove_binlog(index_record)) {
          OMS_ERROR("Deletion of the last binlog file [{}] failed, there is no complete mapping relationship",
              index_record.get_file_name());
          return OMS_FAILED;
        }
        OMS_INFO("Delete incomplete breakpoint binlog file: {}", index_record.get_file_name());
        return OMS_OK;
      }

      error_code err;
      fs::resize_file(index_record.get_file_name(), trx_overview.trx_specify_truncated_end_pos, err);
      if (err) {
        OMS_ERROR("Failed to intercept transaction data without mapping relationship forward.");
        return OMS_FAILED;
      }

      BinlogIndexRecord truncated_index_record = index_record;
      truncated_index_record.set_position(trx_overview.trx_specify_truncated_end_pos);
      truncated_index_record.set_current_mapping(
          std::pair<std::string, uint64_t>(gtid_seq.get_xid_start(), gtid_seq.get_gtid_start()));
      truncated_index_record.set_checkpoint(gtid_seq.get_commit_version_start());

      g_index_manager->update_index(truncated_index_record);
      OMS_INFO("Correct the index of the breakpoint binlog based last transaction with git [{}] from [{}] to [{}]",
          trx_overview.last_complete_trx_gtid,
          index_record.to_string(),
          truncated_index_record.to_string());
      return OMS_OK;
    }

    // The binlog file is ahead of the index file and the binlog file needs to be truncated.
    BinlogIndexRecord correct_index_record = index_record;
    uint64_t file_size = FsUtil::file_size(index_record.get_file_name());
    // correct binlog index record
    if (binlog_trx.trx_specify_truncated_end_pos != 0) {
      OMS_ERROR("Failed to match gtid mapping for last complete transaction with gtid: {}",
          binlog_trx.last_complete_trx_gtid);
      error_code err;
      fs::resize_file(index_record.get_file_name(), binlog_trx.trx_specify_truncated_end_pos, err);
      if (err) {
        OMS_ERROR("Failed to truncate no mapping relationship found transaction with gtid [{}] in breakpoint binlog "
                  "file [{}] from [{}] to "
                  "[{}], error: {}",
            binlog_trx.trx_specify_truncated_gtid,
            index_record.get_file_name(),
            file_size,
            binlog_trx.trx_specify_truncated_end_pos,
            err.message());
        return OMS_FAILED;
      }

      OMS_INFO("Truncate no mapping relationship found transaction with gtid [{}] in breakpoint binlog file [{}] "
               "from [{}] to [{}]",
          binlog_trx.trx_specify_truncated_gtid,
          index_record.get_file_name(),
          file_size,
          binlog_trx.trx_specify_truncated_end_pos);
      correct_index_record.set_position(binlog_trx.trx_specify_truncated_end_pos);
      correct_index_record.set_current_mapping(index_record.get_before_mapping());
      g_index_manager->update_index(correct_index_record);
      OMS_INFO("Correct the index of the breakpoint binlog based last transaction with git [{}] from [{}] to [{}]",
          binlog_trx.last_complete_trx_gtid,
          index_record.to_string(),
          correct_index_record.to_string());

      return OMS_OK;
    }

    // If the file mapped in the index is not found, it means that the binlog file is invalid and will be deleted
    // directly.Just start reshaping from the previous binlog file
    if (OMS_OK != g_index_manager->remove_binlog(index_record)) {
      OMS_ERROR(
          "Failed to delete the last binlog file [{}] without previous_gtids event", index_record.get_file_name());
      return OMS_FAILED;
    }
    OMS_INFO("Delete incomplete breakpoint binlog file: {}", index_record.get_file_name());
    return OMS_OK;
  }
}
