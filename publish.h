
#ifndef EVENTVIEW_PUBLISH_H
#define EVENTVIEW_PUBLISH_H

#include <assert.h>
#include <variant>
#include <thread>
#include <atomic>
#include <future>

#include "types.h"
#include "opdispatch.h"

namespace eventview {

    template<std::uint32_t NumThreads>
    class Publisher {

    public:
        explicit Publisher(std::shared_ptr<OpDispatch<NumThreads>> dispatch) : dispatch_{dispatch} {}

        Publisher(const Publisher &other) = delete;

        Publisher &operator=(const Publisher &) = delete;

        Publisher(Publisher &&) = default;

        Publisher &operator=(Publisher &&) = default;

        ~Publisher() = default;


        inline void publish(Event &&evt);

    private:

        std::shared_ptr<OpDispatch<NumThreads>> dispatch_;
    };

    template<std::uint32_t NumThreads>
    inline void Publisher<NumThreads>::publish(Event &&evt) {
        auto future = dispatch_->publish_event(std::move(evt));
        future.get(); //return void or throw transferred exception
    }
}

#endif //EVENTVIEW_PUBLISH_H
