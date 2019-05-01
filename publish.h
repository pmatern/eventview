
#ifndef EVENTVIEW_PUBLISH_H
#define EVENTVIEW_PUBLISH_H

#include <assert.h>
#include <variant>
#include <thread>
#include <atomic>
#include <future>
#include <optional>

#include "types.h"
#include "opdispatch.h"

namespace eventview {

    class PublishResult final {
    public:
        explicit PublishResult(std::string error) : error_{std::move(error)} {}

        PublishResult() = default;

        PublishResult(const PublishResult &) = default;

        PublishResult &operator=(const PublishResult &) = default;

        PublishResult(PublishResult &&) noexcept = default;

        PublishResult &operator=(PublishResult &&) = default;

        ~PublishResult() = default;

        explicit operator bool() const {
            return !error_;
        }

        const std::string error() const {
            return *error_;
        }

    private:
        std::optional<std::string> error_;
    };

    const PublishResult PUB_SUCCESS{};

    template<std::uint32_t NumThreads>
    class Publisher {

    public:
        explicit Publisher(std::shared_ptr<OpDispatch<NumThreads>> dispatch) : dispatch_{dispatch} {}

        Publisher(const Publisher &other) = delete;

        Publisher &operator=(const Publisher &) = delete;

        Publisher(Publisher &&) = default;

        Publisher &operator=(Publisher &&) = default;

        ~Publisher() = default;

        inline PublishResult publish(Event &&evt) noexcept ;

    private:
        std::shared_ptr<OpDispatch<NumThreads>> dispatch_;
    };

    template<std::uint32_t NumThreads>
    inline PublishResult Publisher<NumThreads>::publish(Event &&evt) noexcept {
        try {
            auto future = dispatch_->publish_event(std::move(evt));
            future.get(); //return void or throw transferred exception
            return PUB_SUCCESS;

        } catch (std::exception &e) {
            return PublishResult{e.what()};
        } catch (...) {
            return PublishResult{"unexpected exception"};
        }
    }
}

#endif //EVENTVIEW_PUBLISH_H
