#pragma once
namespace Disruptor
{

    /**
     * Callback interface to be implemented for processing units of work as they become available in the RingBuffer<T>
     *
     * \tparam T event implementation storing the data for sharing during exchange or parallel coordination of an event.
     */ 
    template <class T>
    class IWorkHandler
    {
    public:
        virtual ~IWorkHandler() = default;

        /**
         * Callback to indicate a unit of work needs to be processed.
         *
         * \param evt event published to the RingBuffer<T>
         * \param sequence
         */
        virtual void onEvent(T& evt, std::int64_t sequence) = 0;
    };

} // namespace Disruptor
