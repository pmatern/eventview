//
// Created by Matern, Pete on 2019-04-23.
//

#ifndef EVENTVIEW_PUBLISH_H
#define EVENTVIEW_PUBLISH_H

#include <assert.h>
#include <variant>

#include "eventview.h"
#include "expected.h"
#include "entitystorage.h"

namespace eventview {

    class Publisher {

    public:
        explicit Publisher(std::shared_ptr<EntityStore> store): store_{std::move(store)} {}

        Publisher(const Publisher &other) = delete;

        Publisher &operator=(const Publisher &) = delete;

        Publisher(Publisher &&) = default;

        Publisher &operator=(Publisher &&) = default;

        ~Publisher() = default;



        nonstd::expected<void, std::string> publish(Event evt) {
            auto result = store_->put(evt.id, evt.entity);

            if (result) {
                //remove old referencers
                for (auto& kv : result.value()) {
                    auto& lookup = store_->get(kv.second);
                    if (lookup) {
                        auto &node = lookup->get();
                        auto remove_result = node.remove_referencer(evt.id, kv.first, evt.entity.descriptor);
                        if (!remove_result) {
                            return remove_result.get_unexpected();
                        }
                    } else {
                        //need to add stub storage node for not-yet existent entity and remove ref it
                        auto ref_result = reference_stub(kv.second, evt.id, kv.first, evt.entity.descriptor, false);
                        if (!ref_result) {
                            return ref_result.get_unexpected();
                        }
                    }
                }

                //add new referencers
                for (auto& kv : evt.entity.node) {
                    if (std::holds_alternative<EntityDescriptor>(kv.second)) {
                        auto& desc = *std::get_if<EntityDescriptor>(&kv.second);

                        auto& lookup = store_->get(desc);
                        if (lookup) {
                            auto &node = lookup->get();
                            auto add_result = node.add_referencer(evt.id, kv.first, evt.entity.descriptor);
                            if (!add_result) {
                                return add_result.get_unexpected();
                            }
                        } else {
                            //need to add stub storage node for not-yet existent entity and ref it
                            auto ref_result = reference_stub(desc, evt.id, kv.first, evt.entity.descriptor, true);
                            if (!ref_result) {
                                return ref_result.get_unexpected();
                            }
                        }
                    }
                }

            } else {
                return result.get_unexpected();
            }

            return {};
        }

    private:

        nonstd::expected<void, std::string> reference_stub(EntityDescriptor stub, EventID ref_time,
                const std::string& field, EntityDescriptor ref, bool add_ref) {

            //super low write time ensures whatever delayed write comes in will apply
            auto put_result = store_->put(1, {stub, {}});
            if (put_result) {
                auto &node = store_->get(stub);
                assert(node);

                if (add_ref) {
                    auto add_result = node->get().add_referencer(ref_time, field, ref);
                    if (!add_result) {
                        return add_result.get_unexpected();
                    }
                } else {
                    auto remove_result = node->get().remove_referencer(ref_time, field, ref);
                    if (!remove_result) {
                        return remove_result.get_unexpected();
                    }
                }

                return {};

            } else {
                return put_result.get_unexpected();
            }
        }

        std::shared_ptr<EntityStore> store_;
    };

}

#endif //EVENTVIEW_PUBLISH_H
