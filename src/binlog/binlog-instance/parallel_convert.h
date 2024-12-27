//
// Created by 花轻 on 2024/10/24.
//

#ifndef PARALLEL_CONVERT_H
#define PARALLEL_CONVERT_H

#include "blocking_queue.hpp"
#include "disruptor/Disruptor.h"

#include "thread.h"
#include "timer.h"
#include "str_array.h"
#include "obcdc_config.h"
#include "codec/message.h"
#include "obcdcaccess/obcdc_factory.h"

#include "binlog/ob_log_event.h"
#include "binlog/binlog_index.h"
#include "binlog/ddl-parser/ddl_parser.h"
#include "table_cache.h"

#include <RoundRobinThreadAffinedTaskScheduler.h>
namespace oceanbase::binlog {
class BinlogConverter;
class ParallelConvert;
struct BinlogEvent {
  // data before conversion
  std::vector<ILogRecord*> records;

  // converted data
  std::vector<ObLogEvent*> events;
};

inline BinlogEvent create_binlog_event()
{
  return {};
}
struct BinlogEventHandler final : Disruptor::IEventHandler<BinlogEvent> {
  void onEvent(BinlogEvent& data, std::int64_t sequence, bool endOfBatch) override;

public:
  explicit BinlogEventHandler(BlockingQueue<ObLogEvent*>& queue) : queue(queue)
  {}
  BlockingQueue<ObLogEvent*>& queue;
};

struct BinlogEventConvertHandler final : Disruptor::IWorkHandler<BinlogEvent> {
  void onEvent(BinlogEvent& data, std::int64_t sequence) override;

public:
  explicit BinlogEventConvertHandler(DdlParser& ddl_parser, TableCache& table_cache, IObCdcAccess*& obcdc_access)
      : ddl_parser(ddl_parser), table_cache(table_cache), obcdc_access(obcdc_access)
  {}
  DdlParser& ddl_parser;
  TableCache& table_cache;
  IObCdcAccess*& obcdc_access;
};

struct ConvertExceptionHandler final : Disruptor::IExceptionHandler<BinlogEvent> {
public:
  void handleEventException(const std::exception& ex, std::int64_t sequence, BinlogEvent& evt) override;

  void handleOnStartException(const std::exception& ex) override;

  void handleOnShutdownException(const std::exception& ex) override;

  void handleOnTimeoutException(const std::exception& ex, std::int64_t sequence) override;

  explicit ConvertExceptionHandler(BinlogConverter& converter) : converter(converter)
  {}

  BinlogConverter& converter;
};

bool parallel_convert_gtid_log_event(ILogRecord* record, std::vector<ObLogEvent*>& events,
    binlog::DdlParser& ddl_parser, TableCache& table_cache, bool is_ddl = false);

void parallel_convert_query_event(
    ILogRecord* record, std::vector<ObLogEvent*>& events, binlog::DdlParser& ddl_parser, TableCache& table_cache);

bool parallel_ddl_need_to_be_stored(ILogRecord* record, binlog::DdlParser& ddl_parser);

void parallel_convert_xid_event(ILogRecord* record, std::vector<ObLogEvent*>& events);

void parallel_convert_table_map_event(ILogRecord* record, std::vector<ObLogEvent*>& events, TableCache& table_cache);

void parallel_convert_write_rows_event(ILogRecord* record, std::vector<ObLogEvent*>& events, TableCache& table_cache);

void parallel_convert_delete_rows_event(ILogRecord* record, std::vector<ObLogEvent*>& events, TableCache& table_cache);

void parallel_convert_update_rows_event(ILogRecord* record, std::vector<ObLogEvent*>& events, TableCache& table_cache);

void parallel_pass_heartbeat_checkpoint(ILogRecord* record, std::vector<ObLogEvent*>& events);

void parallel_do_convert(int64_t seq, shared_ptr<Disruptor::disruptor<BinlogEvent>>& disruptor, IObCdcAccess*& obcdc,
    DdlParser& ddl_parser, TableCache& table_cache);

class BinlogConverter;
class ParallelConvert : public Thread {
public:
  ParallelConvert(
      BinlogConverter& converter, BlockingQueue<ILogRecord*>& rqueue, BlockingQueue<ObLogEvent*>& event_queue);

  int init(IObCdcAccess* obcdc);

  void stop() override;

  void run() override;

private:
  shared_ptr<Disruptor::disruptor<BinlogEvent>> _disruptor;
  shared_ptr<BinlogEventHandler> _binlog_event_handler;
  std::vector<shared_ptr<Disruptor::IWorkHandler<BinlogEvent>>> _convert_handler;
  shared_ptr<Disruptor::RoundRobinThreadAffinedTaskScheduler> _task_scheduler;
  BinlogConverter& _converter;
  IObCdcAccess* _obcdc;
  BlockingQueue<ILogRecord*>& _rqueue;
  BlockingQueue<ObLogEvent*>& _event_queue;
  Timer _stage_timer;
  binlog::DdlParser _ddl_parser;
  TableCache _table_cache;
};

}  // namespace oceanbase::binlog

#endif  // PARALLEL_CONVERT_H
