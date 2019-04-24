
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

        nonstd::expected<EventID, std::string> write_event(EventEntity evt) {

            auto evt_id = snowflakes_.next();
            if (0 == evt.descriptor.id) {
                evt.descriptor.id = evt_id;
            }

            auto result = log_.append(Event{evt_id, std::move(evt)});

            if (result) {
                return evt_id;
            } else {
                return nonstd::make_unexpected(result.error());
            }
        }

        EventID next_id() {
            return snowflakes_.next();
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
