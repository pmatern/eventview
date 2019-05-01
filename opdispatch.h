//
// Created by Matern, Pete on 2019-04-29.
//

#ifndef EVENTVIEW_OPDISPATCH_H
#define EVENTVIEW_OPDISPATCH_H

#include <variant>
#include <future>
#include <thread>
#include <chrono>
#include <optional>

#include "types.h"
#include "mpsc.h"

namespace eventview {

    struct Operation {
        std::variant<Event, ViewDescriptor> op;
        std::variant<std::promise<void>, std::promise<std::optional<View> > > res;

        Operation()=default;
        Operation(const Operation &)=delete;
        Operation& operator=(const Operation &)=delete;
        Operation(Operation &&)=default;
        Operation& operator=(Operation &&)=default;
        ~Operation()=default;


        bool is_write() {
            return std::holds_alternative<Event>(op);
        }

        Event take_write() {
            return std::move(*std::get_if<Event>(&op));
        }

        std::promise<void> take_write_res() {
            return std::move(*std::get_if<std::promise<void> >(&res));
        }

        bool is_read() {
            return std::holds_alternative<ViewDescriptor>(op);
        }

        ViewDescriptor take_read() {
            return std::move(*std::get_if<ViewDescriptor>(&op));
        }

        std::promise<std::optional<View>> take_read_res() {
            return std::move(*std::get_if<std::promise<std::optional<View>>>(&res));
        }
    };


    using EventPublishCallback = std::function<void(Event &&evt)>;
    using ViewReadCallback = std::function<const std::optional<View> (const ViewDescriptor &view_desc)>;


    template<std::uint32_t NumThreads>
    class OpDispatch {

    public:
        OpDispatch(EventPublishCallback pub, ViewReadCallback read) : pub_{std::move(pub)}, read_{std::move(read)},
        running_{true}, worker_{ [&]{ work(); } }  {}

        OpDispatch(const OpDispatch &) = delete;
        OpDispatch& operator=(const OpDispatch &) = delete;
        OpDispatch(OpDispatch &&) = default;
        OpDispatch& operator=(OpDispatch &&) = default;
        ~OpDispatch() {
            running_.store(false, std::memory_order_release);
            if (worker_.joinable()) {
                worker_.join();
            }
        };


        std::future<void> publish_event(Event &&evt) {
            std::promise<void> p{};
            auto result = p.get_future();

            Operation op{ std::move(evt), std::move(p) };

            mpsc_.produce(std::move(op));

            return std::move(result);
        }

        std::future<std::optional<View>> read_view(ViewDescriptor &&desc) {
            std::promise<std::optional<View>> p{};
            auto result = p.get_future();

            Operation op{ std::move(desc), std::move(p) };

            mpsc_.produce(std::move(op));

            return std::move(result);
        }


    private:

        void work() {
            try {
                while (running_.load(std::memory_order_consume)) {
                    auto op = mpsc_.consume();
                    if (op) {
                        process_op(std::move(*op));
                    } else {
                        //need backoff strategy
                        std::this_thread::sleep_for(std::chrono::milliseconds(250));
                    }
                }
            } catch (...) {
                //TODO get some logging in here
            }
        }

        void process_op(Operation &&op) {
            if (op.is_read()) {
                const auto& desc = op.take_read();
                auto view = op.take_read_res();
                try {
                    auto& resp = read_(desc);
                    view.set_value(std::move(resp));
                } catch (...) {
                    view.set_exception(std::current_exception());
                }
            } else if (op.is_write()) {
                auto evt = op.take_write();
                auto ok = op.take_write_res();
                try {
                    pub_(std::move(evt));
                    ok.set_value();
                } catch (...) {
                    ok.set_exception(std::current_exception());
                }
            } else {
                throw(std::logic_error("unexpected operation type in dispatcher"));
            }
        }

        EventPublishCallback pub_;
        ViewReadCallback read_;
        MPSC<Operation, NumThreads> mpsc_;
        std::thread worker_;
        std::atomic<bool> running_;
    };

}


#endif //EVENTVIEW_OPDISPATCH_H
