//
// Created by Matern, Pete on 2019-04-24.
//

#ifndef EVENTVIEW_QUERY_H
#define EVENTVIEW_QUERY_H

#include <optional>
#include <variant>

#include "eventview.h"
#include "entitystorage.h"

namespace eventview {

    class ViewProcessor {
    public:
        ViewProcessor(std::shared_ptr<EntityStore> store) : store_{std::move(store)}{}

        ViewProcessor(const ViewProcessor &) = delete;

        ViewProcessor& operator=(const ViewProcessor &) = delete;

        ViewProcessor(ViewProcessor &&) = default;

        ViewProcessor& operator=(ViewProcessor &&) = default;

        ~ViewProcessor() = default;

        std::optional<View> query(const ViewDescriptor& view_desc) {
            auto& root_node = store_->get(view_desc.root);

            if (root_node) {
                View view{view_desc.root, {}};

                for (auto& path : view_desc.paths) {
                    process_path_element(path, 0, root_node->get(), view);
                }

                return std::move(view);
            }
            return {};
        }

    private:

        void process_path_element(const ViewPath& path, const ViewPath::size_type& idx, const StorageNode& node, View& view) {
            if (idx < path.size()) {
                auto &elem = path[idx];

                if (elem.is_val()) {
                    load_value(path, elem, node, view);
                } else if (elem.is_ref()) {
                    follow_ref(path, elem, idx, node, view);
                } else if (elem.is_reverse_ref()) {
                    follow_reverse_refs(path, elem, idx, node, view);
                }
            }
        }


        void follow_ref(const ViewPath& path, const PathElement& elem, const ViewPath::size_type& idx,
                    const StorageNode& node, View& view) {

            auto& fields = node.get_fields();
            auto ref = fields.find(elem.name);

            if (ref != fields.end()) {
                if (std::holds_alternative<EntityDescriptor>(ref->second)) {
                    auto& desc = *std::get_if<EntityDescriptor>(&ref->second);

                    if (desc.type == elem.type) {
                        auto &next_node = store_->get(desc);
                        if (next_node) {
                            process_path_element(path, idx + 1, next_node->get(), view);
                        }
                    }
                }
            }
        }

        void follow_reverse_refs(const ViewPath& path, const PathElement& elem, const ViewPath::size_type& idx,
                        const StorageNode& node, View& view) {

            auto &referencers = node.referencers_for_field(elem.name);
            if (referencers) {
                for (auto &ed : referencers.value()) {
                    if (ed.type == elem.type) {
                        auto &next_node = store_->get(ed);
                        if (next_node) {
                            process_path_element(path, idx + 1, next_node->get(), view);
                        }
                    }
                }
            }
        }


        void load_value(const ViewPath& path, const PathElement& path_elem, const StorageNode& node, View& view) {
            auto& fields = node.get_fields();
            auto field_val = fields.find(path_elem.name);

            if (field_val != fields.end()) {
                view.values.push_back({path, field_val->second});
            }
        }

        std::shared_ptr<EntityStore> store_;
    };
}

#endif //EVENTVIEW_QUERY_H
