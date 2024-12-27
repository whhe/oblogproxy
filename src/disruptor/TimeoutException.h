#pragma once

#include "ExceptionBase.h"


namespace Disruptor
{

    DISRUPTOR_DECLARE_EXCEPTION(TimeoutException);

} // namespace Disruptor


#define DISRUPTOR_THROW_TIMEOUT_EXCEPTION() DISRUPTOR_THROW(::Disruptor::TimeoutException, "")
