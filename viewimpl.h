//
// Created by Matern, Pete on 2019-04-30.
//

#ifndef EVENTVIEW_VIEWIMPL_H
#define EVENTVIEW_VIEWIMPL_H

#include <optional>
#include <variant>

#include "types.h"
#include "entitystorage.h"

namespace eventview {

    class ViewReaderImpl {
    public:
        explicit ViewReaderImpl(std::shared_ptr<EntityStore> store) : store_{std::move(store)} {}

        ViewReaderImpl(const ViewReaderImpl &) = delete;

        ViewReaderImpl &operator=(const ViewReaderImpl &) = delete;

        ViewReaderImpl(ViewReaderImpl &&) = default;

        ViewReaderImpl &operator=(ViewReaderImpl &&) = default;

        ~ViewReaderImpl() = default;

        inline const std::optional<View> read_view(const ViewDescriptor &view_desc) const;

    private:

        inline void process_path_element(const ViewPath &path, const ViewPath::size_type &idx,
                                         const StorageNode &node, View &view) const;

        inline void follow_ref(const ViewPath &path, const PathElement &elem, const ViewPath::size_type &idx,
                               const StorageNode &node, View &view) const;

        inline void follow_reverse_refs(const ViewPath &path, const PathElement &elem, const ViewPath::size_type &idx,
                                        const StorageNode &node, View &view) const;

        inline void load_value(const ViewPath &path, const PathElement &path_elem,
                               const StorageNode &node, View &view) const;

        std::shared_ptr<EntityStore> store_;
    };

    inline const std::optional<View> ViewReaderImpl::read_view(const ViewDescriptor &view_desc) const {
        const auto &root_node = store_->get(view_desc.root);

        if (root_node) {
            View view{view_desc.root, {}};

            for (auto &path : view_desc.paths) {
                process_path_element(path, 0, root_node->get(), view);
            }

            return std::move(view);
        }

        return {};

    }


    inline void
    ViewReaderImpl::process_path_element(const ViewPath &path, const ViewPath::size_type &idx, const StorageNode &node,
                                     View &view) const {
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


    inline void ViewReaderImpl::follow_ref(const ViewPath &path, const PathElement &elem, const ViewPath::size_type &idx,
                                       const StorageNode &node, View &view) const {

        auto &fields = node.get_fields();
        auto ref = fields.find(elem.name);

        if (ref != fields.end()) {
            if (ref->second.is_descriptor()) {
                auto &desc = ref->second.as_descriptor();

                if (desc.type == elem.type) {
                    const auto &next_node = store_->get(desc);
                    if (next_node) {
                        process_path_element(path, idx + 1, next_node->get(), view);
                    }
                }
            }
        }
    }


    inline void
    ViewReaderImpl::follow_reverse_refs(const ViewPath &path, const PathElement &elem, const ViewPath::size_type &idx,
                                    const StorageNode &node, View &view) const {

        for (auto &ed : node.referencers_for_field(elem.name)) {
            if (ed.type == elem.type) {
                const auto &next_node = store_->get(ed);
                if (next_node) {
                    process_path_element(path, idx + 1, next_node->get(), view);
                }
            }
        }
    }


    inline void ViewReaderImpl::load_value(const ViewPath &path, const PathElement &path_elem, const StorageNode &node,
                                       View &view) const {
        auto &fields = node.get_fields();
        auto field_val = fields.find(path_elem.name);

        if (field_val != fields.end()) {
            view.values.push_back({path, field_val->second});
        }
    }
}


#endif //EVENTVIEW_VIEWIMPL_H
