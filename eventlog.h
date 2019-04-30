
#ifndef EVENTVIEW_EVENTLOG_H
#define EVENTVIEW_EVENTLOG_H

#include <vector>
#include <exception>
#include <iostream>
#include "types.h"

namespace eventview {

    template<typename Storage = std::vector<Event> >
    class EventLog final {
    public:
        explicit EventLog(EventReceiver receiver) : EventLog(receiver, Storage{}) {}

        EventLog(EventReceiver receiver, Storage storage) : publisher_{std::move(receiver)}, storage_{std::move(storage)} {}

        EventLog(const EventLog &other) = delete;

        EventLog &operator=(const EventLog &) = delete;

        EventLog(EventLog &&) noexcept = default;

        EventLog &operator=(EventLog &&) noexcept = default;

        ~EventLog() = default;


        void append(Event &&evt) {
            /*
             * If storage succeeds, then publish fails, the caller might retry. In that case there will be dupe events in the
             * underlying persistent log. That's okay.
             */
            storage_.push_back(evt);
            publisher_(std::move(evt));
        }

        void replay() {
            for (auto evt : storage_) {
                publisher_(std::move(evt));
            }
        }

    private:
        Storage storage_;
        EventReceiver publisher_;

    };
}

#endif //EVENTVIEW_EVENTLOG_H
