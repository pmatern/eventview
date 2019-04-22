
#ifndef EVENTVIEW_EVENTLOG_H
#define EVENTVIEW_EVENTLOG_H

#include <vector>
#include <exception>
#include <functional>
#include "expected.h"
#include "eventview.h"

namespace eventview {

    using EventReceiver = std::function<nonstd::expected<void, std::string>(Event && evt)>;

    template<typename Storage = std::vector<Event> >
    class EventLog final {
    public:
        explicit EventLog(EventReceiver &receiver) : publisher_{std::move(receiver)} {}

        EventLog(const EventLog &other) = delete;

        EventLog &operator=(const EventLog &) = delete;

        ~EventLog() = default;

        nonstd::expected<void, std::string> append(Event &&evt) {
            Event publish{evt};
            try {
                storage_.push_back(std::move(evt));
            } catch (std::exception &e) {
                return nonstd::make_unexpected(e.what());
            }

            auto result = publisher_(std::move(publish));
            if (result) {
                return {};
            } else {
                return nonstd::make_unexpected(result.error());
            }
        }

        nonstd::expected<void, std::string> replay() {
            for (auto &evt : storage_) {
                auto publish = evt;
                auto result = publisher_(std::move(publish));
                if (result) {
                    return {};
                } else {
                    return result.error();
                }
            }
        }

    private:
        Storage storage_;
        EventReceiver publisher_;

    };
}

#endif //EVENTVIEW_EVENTLOG_H
