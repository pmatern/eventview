//
// Created by Matern, Pete on 2019-04-30.
//

#ifndef EVENTVIEW_PUBLISHIMPL_H
#define EVENTVIEW_PUBLISHIMPL_H

#include <assert.h>
#include <variant>
#include <thread>
#include <atomic>
#include <future>

#include "types.h"
#include "entitystorage.h"

namespace eventview {

    class PublisherImpl {

    public:
        explicit PublisherImpl(std::shared_ptr<EntityStore> store) : store_{std::move(store)} {}

        PublisherImpl(const PublisherImpl &other) = delete;

        PublisherImpl &operator=(const PublisherImpl &) = delete;

        PublisherImpl(PublisherImpl &&) = default;

        PublisherImpl &operator=(PublisherImpl &&) = default;

        ~PublisherImpl() = default;


        inline void publish(Event &&evt);

    private:

        inline void reference_stub(EntityDescriptor stub, EventID ref_time,
                                   const std::string &field, EntityDescriptor ref, bool add_ref);

        std::shared_ptr<EntityStore> store_;
    };

    inline void PublisherImpl::publish(Event &&evt) {
        auto result = store_->put(evt.id, evt.entity);

        //remove old referencers
        for (auto &kv : result) {
            const auto &lookup = store_->get(kv.second);
            if (lookup) {
                auto &node = lookup->get();
                node.remove_referencer(evt.id, kv.first, evt.entity.descriptor);
            } else {
                //need to add stub storage node for not-yet existent entity and remove ref it
                reference_stub(kv.second, evt.id, kv.first, evt.entity.descriptor, false);
            }
        }

        //add new referencers
        for (auto &kv : evt.entity.node) {
            if (kv.second.is_descriptor()) {
                auto &desc = kv.second.as_descriptor();

                const auto &lookup = store_->get(desc);
                if (lookup) {
                    auto &node = lookup->get();
                    node.add_referencer(evt.id, kv.first, evt.entity.descriptor);
                } else {
                    //need to add stub storage node for not-yet existent entity and ref it
                    reference_stub(desc, evt.id, kv.first, evt.entity.descriptor, true);
                }
            }
        }

    }

    inline void PublisherImpl::reference_stub(EntityDescriptor stub, EventID ref_time,
                                          const std::string &field, EntityDescriptor ref, bool add_ref) {
        //super low write time ensures whatever delayed write comes in will apply
        store_->put(1, {stub, {}});
        const auto &node = store_->get(stub);

        if (add_ref) {
            node->get().add_referencer(ref_time, field, ref);
        } else {
            node->get().remove_referencer(ref_time, field, ref);
        }
    }

}


#endif //EVENTVIEW_PUBLISHIMPL_H
