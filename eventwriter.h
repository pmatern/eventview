
#ifndef EVENTVIEW_EVENTWRITER_H
#define EVENTVIEW_EVENTWRITER_H

#include "eventview.h"
#include "snowflake.h"
#include "eventlog.h"
#include "expected.h"

namespace eventview {


    template<typename EvtLog = EventLog<>, typename SFProvider = SnowflakeProvider<> >
    class EventWriter final {
    public:
        EventWriter(std::uint32_t writer_id, EventReceiver receiver) :
                snowflakes_{SFProvider{writer_id}},
                log_{EvtLog{std::move(receiver)}} {}

        nonstd::expected<EventID, std::string> write_event(const EventEntity &evt) {
            EventEntity to_store{evt};

            auto evt_id = snowflakes_.next();
            if (0 == to_store.descriptor.id) {
                to_store.descriptor.id = evt_id;
            }

            auto result = log_.append(Event{evt_id, std::move(to_store)});

            if (result) {
                return evt_id;
            } else {
                return nonstd::make_unexpected(result.error());
            }
        }

        EventWriter(const EventWriter &other) = delete;

        EventWriter &operator=(const EventWriter &) = delete;

        EventWriter(EventWriter &&) = default;

        EventWriter &operator=(EventWriter &&) = default;

        ~EventWriter() = default;

        /*
        EventID write_event(EventEntity evt, ViewCallback callback) {

        }
         */
    private:
        EvtLog log_;
        SFProvider snowflakes_;
    };
}

#endif //EVENTVIEW_EVENTWRITER_H
