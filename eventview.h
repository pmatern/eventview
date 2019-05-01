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

        auto reader_impl_ptr = std::make_shared<ViewReaderImpl>(ViewReaderImpl{store});

        auto pub_impl_ptr = std::make_shared<PublisherImpl>(PublisherImpl{store});

        EventPublishCallback pub_cb = [=](Event &&evt) {
            pub_impl_ptr->publish(std::move(evt));
        };

        ViewReadCallback view_cb = [=](const ViewDescriptor &view_desc) -> const std::optional<View> {
            return reader_impl_ptr->read_view(view_desc);
        };

        OpDispatch<NumThreads> dispatch{pub_cb, view_cb};

        auto dispatch_ptr = dispatch.shared_from_this(); //std::make_shared<OpDispatch<NumThreads>>(OpDispatch<NumThreads>{pub_cb, view_cb});
        Publisher<NumThreads> pub{dispatch_ptr};
        ViewReader reader{dispatch_ptr};

        return {std::move(pub), std::move(reader)};
    };

    template<std::uint32_t NumThreads, typename LogStorage = std::vector<Event>>
    EventWriter<LogStorage> make_writer(std::uint32_t writer_id, Publisher<NumThreads> &publisher) {

        auto pub_ptr = publisher.shared_from_this();

        return EventWriter<LogStorage>{writer_id, [=](Event &&evt) {
            pub_ptr->publish(std::move(evt));
        }};
    }
}

#endif //EVENTVIEW_EVENTVIEW_H
