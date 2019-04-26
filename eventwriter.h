
#ifndef EVENTVIEW_EVENTWRITER_H
#define EVENTVIEW_EVENTWRITER_H

#include <variant>
#include "eventview.h"
#include "snowflake.h"
#include "eventlog.h"

namespace eventview {

    class WriteResult final {
    public:
        WriteResult(EventID id) : result_{id} {}

        WriteResult(std::string error_msg) : result_{std::move(error_msg)} {}

        WriteResult(const WriteResult &) = default;

        WriteResult &operator=(const WriteResult &) = default;

        WriteResult(WriteResult &&) noexcept = default;

        WriteResult &operator=(WriteResult &&) = default;

        ~WriteResult() = default;

        explicit operator bool() const {
            return std::holds_alternative<EventID>(result_);
        }

        const EventID event_id() const {
            return *std::get_if<EventID>(&result_);
        }

        const std::string &error() const {
            return *std::get_if<std::string>(&result_);
        }

    private:
        std::variant<EventID, std::string> result_;
    };


    template<typename EvtLog = EventLog<>, typename SFProvider = SnowflakeProvider<> >
    class EventWriter final {
    public:
        EventWriter(std::uint32_t writer_id, EventReceiver receiver) :
                snowflakes_{SFProvider{writer_id}},
                log_{EvtLog{std::move(receiver)}} {}

        inline const WriteResult write_event(EventEntity evt) noexcept;

        EventID next_id() {
            return snowflakes_.next();
        }

        EventWriter(const EventWriter &) = delete;

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

    template<>
    inline const WriteResult EventWriter<>::write_event(EventEntity evt) noexcept {

        auto evt_id = snowflakes_.next();
        if (0 == evt.descriptor.id) {
            evt.descriptor.id = evt_id;
        }

        try {
            log_.append(Event{evt_id, std::move(evt)});
            return evt_id;
        } catch (std::exception &e) {
            return {e.what()};
        } catch (...) {
            return {"unexpected exception"};
        }
    }
}

#endif //EVENTVIEW_EVENTWRITER_H
