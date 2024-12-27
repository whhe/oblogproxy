//
// Created by 花轻 on 2024/11/13.
//

#include "release_event_helper.h"

namespace oceanbase::binlog {
void ReleaseEventHandler::onEvent(ReleaseEvent& data, std::int64_t sequence)
{
  release_vector(data.events);
}
}