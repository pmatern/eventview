
#ifndef EVENTVIEW_VIEW_H
#define EVENTVIEW_VIEW_H

#include <optional>
#include <variant>

#include "eventview.h"
#include "entitystorage.h"

namespace eventview {

    class ViewReader {
    public:
        explicit ViewReader(std::shared_ptr<EntityStore> store) : store_{std::move(store)} {}

        ViewReader(const ViewReader &) = delete;

        ViewReader &operator=(const ViewReader &) = delete;

        ViewReader(ViewReader &&) = default;

        ViewReader &operator=(ViewReader &&) = default;

        ~ViewReader() = default;

        inline const std::optional<View> read_view(const ViewDescriptor &view_desc) const noexcept;

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

    inline const std::optional<View> ViewReader::read_view(const ViewDescriptor &view_desc) const noexcept {
        try {
            const auto &root_node = store_->get(view_desc.root);

            if (root_node) {
                View view{view_desc.root, {}};

                for (auto &path : view_desc.paths) {
                    process_path_element(path, 0, root_node->get(), view);
                }

                return std::move(view);
            }
            return {};
        } catch (std::exception &e) {
            //do something
            return {};
        } catch (...) {
            //do something else?
            return {};
        }
    }


    inline void
    ViewReader::process_path_element(const ViewPath &path, const ViewPath::size_type &idx, const StorageNode &node,
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


    inline void ViewReader::follow_ref(const ViewPath &path, const PathElement &elem, const ViewPath::size_type &idx,
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
    ViewReader::follow_reverse_refs(const ViewPath &path, const PathElement &elem, const ViewPath::size_type &idx,
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


    inline void ViewReader::load_value(const ViewPath &path, const PathElement &path_elem, const StorageNode &node,
                                          View &view) const {
        auto &fields = node.get_fields();
        auto field_val = fields.find(path_elem.name);

        if (field_val != fields.end()) {
            view.values.push_back({path, field_val->second});
        }
    }
}

#endif //EVENTVIEW_VIEW_H
