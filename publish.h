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
                        node.remove_referencer(evt.id, kv.first, evt.entity.descriptor); //TODO check retval
                    } else {
                        //need to add stub storage node for not-yet existent entity
                    }
                }

                //add new referencers
                for (auto& kv : evt.entity.node) {
                    if (std::holds_alternative<EntityDescriptor>(kv.second)) {
                        auto& desc = *std::get_if<EntityDescriptor>(&kv.second);

                        auto& lookup = store_->get(desc);
                        if (lookup) {
                            auto &node = lookup->get();
                            node.add_referencer(evt.id, kv.first, evt.entity.descriptor); //TODO check retval
                        } else {
                            //need to add stub storage node for not-yet existent entity
                        }
                    }
                }

            } else {
                return result.get_unexpected();
            }

            return {};
        }

    private:
        std::shared_ptr<EntityStore> store_;
    };

}

#endif //EVENTVIEW_PUBLISH_H
