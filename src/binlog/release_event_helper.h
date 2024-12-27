//
// Created by 花轻 on 2024/11/13.
//

#ifndef RELEASE_EVENT_HELPER_H
#define RELEASE_EVENT_HELPER_H
#include "ob_log_event.h"
#include <vector>
#include <IEventHandler.h>
#include <Disruptor.h>
#include <RoundRobinThreadAffinedTaskScheduler.h>

namespace oceanbase::binlog {
struct ReleaseEvent {
  // converted data
  std::vector<ObLogEvent*> events;
};

inline ReleaseEvent create_release_event()
{
  return ReleaseEvent{};
}

struct ReleaseEventHandler : Disruptor::IWorkHandler<ReleaseEvent> {
  void onEvent(ReleaseEvent& data, std::int64_t sequence) override;
};
}  // namespace oceanbase::binlog

#endif  // RELEASE_EVENT_HELPER_H
