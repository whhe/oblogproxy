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

#ifndef OMS_LOGPROXY_BINLOG_STORAGE_H
#define OMS_LOGPROXY_BINLOG_STORAGE_H

#include <cassert>

#include "thread.h"
#include "timer.h"
#include "log.h"
#include "ob_log_event.h"
#include "blocking_queue.hpp"
#include "obcdcaccess/obcdc/obcdc_entry.h"
#include "binlog_index.h"
#include "data_type.h"
#include "obcdc_config.h"
#include "gtid_manager.h"
#include "rate_limiter.h"
#include "common/buffer_manager.hpp"

#include <Disruptor.h>
#include <RoundRobinThreadAffinedTaskScheduler.h>
#include <env.h>

namespace oceanbase::binlog {

class BinlogConverter;
class BinlogStorage;

struct SerializeEvent {
  std::vector<ObLogEvent*> events;
  BufferManager buffer;
  bool is_rotation = false;
  std::vector<uint64_t> rotate_offsets;
  std::vector<ObLogEvent*> rotate_events;
  // Used to record the current snapshot status
  BinlogIndexRecord index_record;

public:
  explicit SerializeEvent() : events(), buffer(2 * 1024 * 1024, 8 * 1024), rotate_offsets(), rotate_events()
  {}
  explicit SerializeEvent(const uint64_t capacity, const size_t growth_size)
      : events(), buffer(capacity, growth_size), rotate_offsets(), rotate_events()
  {}

  void reset();
};

inline SerializeEvent create_serialize_event()
{
  return SerializeEvent(s_meta.binlog_config()->preallocated_memory_bytes(),
      s_meta.binlog_config()->preallocated_expansion_memory_bytes());
}

// for concurrent serialization
struct SerializeHandler final : Disruptor::IWorkHandler<SerializeEvent> {
  void onEvent(SerializeEvent& data, std::int64_t sequence) override;

public:
  explicit SerializeHandler() = default;
};

// Used for serial allocation of global gtid and backfill event position
struct AllocateHandler : Disruptor::IEventHandler<SerializeEvent> {
  void onEvent(SerializeEvent& data, std::int64_t sequence, bool endOfBatch) override;
  explicit AllocateHandler(BinlogStorage* binlog_storage) : _binlog_storage(binlog_storage)
  {}

  BinlogStorage* _binlog_storage;
};

void purge_expired_binlog();
// used to persist binlog files
struct StorageHandler : Disruptor::IEventHandler<SerializeEvent> {
  void onEvent(SerializeEvent& data, std::int64_t sequence, bool endOfBatch) override;
  explicit StorageHandler(BinlogStorage* binlog_storage) : _binlog_storage(binlog_storage)
  {}

  BinlogStorage* _binlog_storage;
};

struct SerializeExceptionHandler final : Disruptor::IExceptionHandler<SerializeEvent> {
public:
  void handleEventException(const std::exception& ex, std::int64_t sequence, SerializeEvent& evt) override;

  void handleOnStartException(const std::exception& ex) override;

  void handleOnShutdownException(const std::exception& ex) override;

  void handleOnTimeoutException(const std::exception& ex, std::int64_t sequence) override;

  explicit SerializeExceptionHandler(BinlogStorage* binlog_storage) : _binlog_storage(binlog_storage)
  {}

  BinlogStorage* _binlog_storage;
};

class BinlogStorage : public Thread {
public:
  int init();

  void stop() override;

  void run() override;

  BinlogStorage(BinlogConverter& reader, BlockingQueue<ObLogEvent*>& event_queue);

  int64_t rotate(ObLogEvent* event, MsgBuf& content, std::size_t size, BinlogIndexRecord& index_record);

  int finishing_touches(MsgBuf& buffer, size_t& buffer_pos) const;
  /*
   The rotation action is for the previous binlog file
   */
  int rotate_current_binlog(MsgBuf& content, size_t size, RotateEvent* rotate_event, BinlogIndexRecord& index_record);

  /*
   *The rotation action is for the current binlog file
   */
  int add_next_binlog_index(BinlogIndexRecord& index_record, const RotateEvent* rotate_event);

  int64_t init_binlog_file(RotateEvent* rotate_event);

  int64_t init_next_binlog_file(RotateEvent* rotate_event);

  int global_allocation_of_backfill_events(SerializeEvent& event_batch);

  int64_t persistent_binlog(unsigned char* buffer, size_t size, BinlogIndexRecord& index_record);

public:
  EventRateLimiter& get_rate_limiter()
  {
    return _rate_limiter;
  }

  static void purge_expired_binlog(BinlogIndexRecord& index_record);

private:
  void update_index_record(BinlogIndexRecord& index_record);

  int init_restart_point(const BinlogIndexRecord& index_record, GtidLogEvent* event);

  int recover_safely();

  int64_t rotate_binlog_file(uint64_t timestamp, SerializeEvent& event);

  void merge_previous_gtids(uint64_t last_gtid, std::vector<GtidMessage*>& previous_gtids);

  void set_start_pos(const std::vector<GtidMessage*>& previous_gtids);

private:
  BlockingQueue<ObLogEvent*>& _event_queue;
  std::string _file_name;
  Timer _stage_timer;
  BinlogConverter& _converter;
  uint64_t _offset{};
  bool _valid_events_coming = false;

  EventRateLimiter _rate_limiter;

  uint64_t _txn_gtid_seq = 1;
  uint64_t _xid = 0;
  uint32_t _cur_pos = 0;
  uint64_t _binlog_file_index = 0;
  std::pair<std::string /* ob_txn_id */, uint64_t /* ob_txn_gtid_seq */> _txn_mapping;
  bool _filter_util_checkpoint_trx = true;
  bool _meet_initial_trx = false;
  bool _filter_checkpoint_trx = true;
  uint64_t _start_timestamp = 0;
  // binlog::DdlParser _ddl_parser;
  uint64_t _first_gtid_seq = 1;
  std::vector<GtidMessage*> _previous_gtid_messages;
  uint64_t _last_record_ts = 0;
  shared_ptr<Disruptor::disruptor<SerializeEvent>> _serialize_disruptor;
  shared_ptr<AllocateHandler> _allocate_handler;
  shared_ptr<StorageHandler> _storage_handler;
  std::vector<shared_ptr<Disruptor::IWorkHandler<SerializeEvent>>> _serialize_handlers;
  shared_ptr<Disruptor::RoundRobinThreadAffinedTaskScheduler> _serialize_task_scheduler;

  BinlogIndexRecord _index_record;
};
}  // namespace oceanbase::binlog

#endif  // OMS_LOGPROXY_BINLOG_STORAGE_H
