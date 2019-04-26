//
// Created by Matern, Pete on 2019-04-20.
//

#ifndef EVENTVIEW_ENTITYSTORAGE_H
#define EVENTVIEW_ENTITYSTORAGE_H

#include "eventview.h"
#include <unordered_map>
#include <string>
#include <vector>
#include <exception>
#include <variant>
#include <optional>

namespace eventview {

    struct Existence {
        EventID add_time;
        EventID remove_time;

        bool exists() {
            return add_time > remove_time;
        }

        void touch(EventID touch_time) {
            if (touch_time > add_time) {
                add_time = touch_time;
            }
        }

        void deref(EventID deref_time) {
            if (deref_time > remove_time) {
                remove_time = deref_time;
            }
        }
    };

    using ReferenceSet = std::unordered_map<EntityDescriptor, Existence>;
    using RemovedReferences = std::unordered_map<std::string, EntityDescriptor>;

    class StorageNode final {

    public:
        StorageNode(EventID write_time, EventEntity initial_state) : existence_{Existence{write_time, 0}},
                                                                     entity_{std::move(initial_state)},
                                                                     referencers_{{}} {}


        StorageNode(const StorageNode &other) = delete;

        StorageNode &operator=(const StorageNode &) = delete;

        StorageNode(StorageNode &&) noexcept = default;

        StorageNode &operator=(StorageNode &&) noexcept = default;

        ~StorageNode() = default;

        EntityTypeID type() {
            return entity_.descriptor.type;
        }

        void add_referencer(EventID write_time, const std::string &field_name, EntityDescriptor referencer) {
            auto& refs_by_field = referencers_[field_name];

            auto& found = refs_by_field[referencer];
            found.touch(write_time);
            existence_.touch(write_time);
        }

        void remove_referencer(EventID write_time, const std::string &field_name, EntityDescriptor referencer) {
            auto& refs_by_field = referencers_[field_name];

            auto& found = refs_by_field[referencer];
            found.deref(write_time);
            existence_.touch(write_time);
        }

        std::vector<EntityDescriptor> referencers_for_field(const std::string &field) const {
            std::vector<EntityDescriptor> snapshot;

            auto refs_by_field = referencers_.find(field);
            if (refs_by_field != referencers_.end()) {
                auto field_refs = refs_by_field->second;

                for (auto kv : field_refs) {
                    if (kv.second.exists()) {
                        snapshot.push_back(kv.first);
                    }
                }
            }

            return std::move(snapshot);
        }

        RemovedReferences update_fields(EventID update_time, EventEntity update) {
            std::unordered_map<std::string, EntityDescriptor> snapshot;

            if (update_time > existence_.add_time && update.descriptor == entity_.descriptor) {
                ValueNode to_deref{entity_.node};

                for (auto& kv : to_deref) {
                    if (kv.second.is_descriptor()) {
                        snapshot[kv.first] = kv.second.as_descriptor();
                    }
                }

                entity_.node = update.node;
                existence_.touch(update_time);
            }

            return std::move(snapshot);
        }

        const ValueNode get_fields() const {
            return entity_.node;
        }

        bool exists() {
            return existence_.exists();
        }

        void deref(EventID deref_time) {
            existence_.deref(deref_time);
        }

    private:
        Existence existence_;
        EventEntity entity_;
        std::unordered_map<std::string, ReferenceSet> referencers_;
    };


    class EntityStore final {
    public:
        EntityStore() = default;

        EntityStore(const EntityStore &other) = delete;

        EntityStore &operator=(const EntityStore &) = delete;

        EntityStore(EntityStore &&) noexcept = default;

        EntityStore &operator=(EntityStore &&) noexcept = default;

        ~EntityStore() = default;

        RemovedReferences put(EventID write_time, EventEntity entity) {
            auto found = store_.find(entity.descriptor.id);

            if (found == store_.end()) {
                store_.insert(std::make_pair(entity.descriptor.id, StorageNode{write_time, std::move(entity)}));
                return {};
            } else {
                return found->second.update_fields(write_time, std::move(entity));
            }

        }

        const std::optional<std::reference_wrapper<StorageNode> > get(const EntityDescriptor &descriptor) {
            auto found = store_.find(descriptor.id);

            if (found != store_.end()) {
                StorageNode &node = found->second;
                if (node.type() == descriptor.type) {
                    return node;
                }
            }

            return {};
        }

    private:
        std::unordered_map<EntityID, StorageNode> store_;
    };

}

#endif //EVENTVIEW_ENTITYSTORAGE_H
