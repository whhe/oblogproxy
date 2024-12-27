#pragma once

#include "IDataProvider.h"
#include "ISequenced.h"


namespace Disruptor
{

    template <class T>
    class IEventSequencer : public IDataProvider< T >, public ISequenced
    {
    };

} // namespace Disruptor
