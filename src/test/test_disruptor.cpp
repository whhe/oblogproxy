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
#include "binlog/binlog-instance/binlog_convert.h"
#include "binlog/data_type.h"

#include <Disruptor.h>
#include <RoundRobinThreadAffinedTaskScheduler.h>
#include <ThreadPerTaskScheduler.h>

using namespace oceanbase::binlog;
struct LongEvent {
  long value;
};

struct PrintingEventHandler : Disruptor::IEventHandler<LongEvent> {
  explicit PrintingEventHandler(int toProcess) : m_actuallyProcessed(0), m_toProcess(toProcess)
  {}

  void onEvent(LongEvent& event, int64_t, bool) override
  {
    std::cout << "Event: " << event.value << std::endl;

    if (++m_actuallyProcessed == m_toProcess)
      m_allDone.notify_all();
  }

  void waitEndOfProcessing()
  {
    std::unique_lock<decltype(m_mutex)> lk(m_mutex);
    m_allDone.wait(lk);
  }

private:
  std::mutex m_mutex;
  std::condition_variable m_allDone;
  int m_toProcess;
  int m_actuallyProcessed;
};

TEST(Disruptor, event)
{
  auto const ExpectedNumberOfEvents = 10000;
  auto const RingBufferSize = 1024;
  shared_ptr<Disruptor::disruptor<LongEvent>> disruptor;
  shared_ptr<PrintingEventHandler> printingEventHandler;
  shared_ptr<Disruptor::RoundRobinThreadAffinedTaskScheduler> taskScheduler;
  {
    // Instantiate and start the disruptor
    auto eventFactory = []() { return LongEvent(); };
    taskScheduler = std::make_shared<Disruptor::RoundRobinThreadAffinedTaskScheduler>();

    disruptor = std::make_shared<Disruptor::disruptor<LongEvent>>(eventFactory, RingBufferSize, taskScheduler);
    printingEventHandler = std::make_shared<PrintingEventHandler>(ExpectedNumberOfEvents);

    disruptor->handleEventsWith(printingEventHandler);

    taskScheduler->start(1);
    disruptor->start();
  }

  // Publish events
  auto ringBuffer = disruptor->ringBuffer();
  for (auto i = 0; i < ExpectedNumberOfEvents; ++i) {
    auto nextSequence = ringBuffer->next();
    (*ringBuffer)[nextSequence].value = i;
    ringBuffer->publish(nextSequence);
  }

  // Wait for the end of execution and shutdown
  printingEventHandler->waitEndOfProcessing();

  disruptor->shutdown();
  taskScheduler->stop();
}
