//
// Created by Matern, Pete on 2019-04-30.
//

#ifndef EVENTVIEW_EVENTVIEW_H
#define EVENTVIEW_EVENTVIEW_H

#include <vector>

#include "types.h"
#include "opdispatch.h"
#include "view.h"
#include "viewimpl.h"
#include "publish.h"
#include "publishimpl.h"
#include "eventwriter.h"

namespace eventview {

    template<std::uint32_t NumThreads>
    std::pair<Publisher<NumThreads>, ViewReader<NumThreads>> make_eventview_system() {

        auto store = std::make_shared<EntityStore>();

        auto reader_impl_ptr = std::make_shared<ViewReaderImpl>(store);

        auto pub_impl_ptr = std::make_shared<PublisherImpl>(store);

        EventPublishCallback pub_cb = [=](Event &&evt) {
            pub_impl_ptr->publish(std::move(evt));
        };

        ViewReadCallback view_cb = [=](const ViewDescriptor &view_desc) -> const std::optional<View> {
            return reader_impl_ptr->read_view(view_desc);
        };

        auto dispatch_ptr = std::make_shared<OpDispatch<NumThreads>>(pub_cb, view_cb);

        Publisher<NumThreads> pub{dispatch_ptr};
        ViewReader reader{dispatch_ptr};

        return {std::move(pub), std::move(reader)};
    };


    template<std::uint32_t NumThreads, typename LogStorage = std::vector<Event>>
    EventWriter<LogStorage> make_writer(std::uint32_t writer_id, Publisher<NumThreads> &publisher) {

        return EventWriter<LogStorage>{writer_id, [&](Event &&evt) {
            auto result = publisher.publish(std::move(evt));
            if (!result) {
                //EventWriter expects exceptions thrown internally rather than result objects
                throw std::runtime_error{result.error()};
            }
        }};
    }

    EventReceiver NoOpReceiver = [](const Event &evt){};

    template<typename LogStorage = std::vector<Event>>
    EventWriter<LogStorage> make_writer(std::uint32_t writer_id, const EventReceiver &receiver=NoOpReceiver) {

        return EventWriter<LogStorage>{writer_id, receiver};
    }

    class WriteReadResult final {
    public:
        WriteReadResult(WriteResult result, std::optional<View> view) : result_{std::move(result)},
            view_{std::move(view)}{}

        WriteReadResult(const WriteReadResult &) = default;

        WriteReadResult &operator=(const WriteReadResult &) = default;

        WriteReadResult(WriteReadResult &&) noexcept = default;

        WriteReadResult &operator=(WriteReadResult &&) = default;

        ~WriteReadResult() = default;

        const WriteResult& result() const {
            return result_;
        }

        const std::optional<View>& view() const {
            return view_;
        }

    private:
        WriteResult result_;
        std::optional<View> view_;
    };

    template<std::uint32_t NumThreads, typename LogStorage = std::vector<Event>>
    WriteReadResult write_and_read(EventWriter<LogStorage>& writer, Entity evt,
            const ViewReader<NumThreads> &reader, ViewDescriptor view_desc) noexcept {
        auto result = writer.write_event(evt);

        if (result) {
            auto desc = evt.descriptor();

            if (desc.id == 0) {
                desc.id = result.event_id();
            }
            if (view_desc.root.id == 0) {
                view_desc.root.id = result.event_id();
            }
            if (!view_desc.expectation) {
                view_desc.expectation = {desc, result.event_id()};
            }

            auto view = reader.read_view(view_desc);

            return WriteReadResult(result, view);
        }

        return WriteReadResult(result, {});
    }

}

#endif //EVENTVIEW_EVENTVIEW_H
