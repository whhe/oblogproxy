#pragma once

#include "ExceptionBase.h"


namespace Disruptor
{

    DISRUPTOR_DECLARE_EXCEPTION(NotSupportedException);

} // namespace Disruptor


#define DISRUPTOR_THROW_NOT_SUPPORTED_EXCEPTION() DISRUPTOR_THROW(::Disruptor::NotSupportedException, "")
