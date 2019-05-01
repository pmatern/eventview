
#ifndef EVENTVIEW_EVENTWRITER_H
#define EVENTVIEW_EVENTWRITER_H

#include <variant>
#include <vector>

#include "types.h"
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


    template<typename LogStorage = std::vector<Event> >
    class EventWriter final {
    public:
        EventWriter(std::uint32_t writer_id, EventReceiver receiver) :
                snowflakes_{SnowflakeProvider{writer_id}},
                log_{EventLog<LogStorage>{std::move(receiver)}} {}

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
        EventLog<LogStorage> log_;
        SnowflakeProvider<> snowflakes_;
    };

    template<typename LogStorage>
    inline const WriteResult EventWriter<LogStorage>::write_event(EventEntity evt) noexcept {

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
