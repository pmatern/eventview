#ifndef EVENTVIEW_EVENTVIEW_H
#define EVENTVIEW_EVENTVIEW_H

#include <cstdint>
#include <cmath>
#include <string>
#include <variant>
#include <unordered_map>
#include <vector>
#include <functional>
#include "expected.h"

namespace eventview {

    using Snowflake = std::uint64_t;
    using EventID = std::uint64_t;
    using EntityID = std::uint64_t;
    using EntityTypeID = std::uint64_t;
    using ViewTypeID = std::uint64_t;

    struct EntityDescriptor {
        EntityID id;
        EntityTypeID type;
    };

    bool operator==(const EntityDescriptor &lhs, const EntityDescriptor &rhs) {
        return lhs.id == rhs.id && lhs.type == rhs.type;
    }

    /*  root on employee
     * name    name field
     * age     age field
     * manager->name  name field of manager reference
     *
     *
     * root on manager
     * name    name field
     * manager<-name   name field of anything referencing via manager field   (will be a list)
     *
     * typed references?
     * list field values supported?
     *
     */

    struct PathElement {
        std::string name;
        EntityTypeID type;
        bool forward;

        const bool is_ref() const {
            return type >= 0 && forward;
        }

        const bool is_val() const {
            return type <= 0;
        }

        const bool is_reverse_ref() const {
            return type >= 0 && !forward;
        }
    };

    bool operator==(const PathElement &lhs, const PathElement &rhs) {
        return lhs.name == rhs.name && lhs.type == rhs.type && lhs.forward == rhs.forward;
    }


    using ViewPath = std::vector<PathElement>;


    struct ViewDescriptor {
        EntityDescriptor root;
        std::vector<ViewPath> paths;
    };


    using PrimitiveFieldValue = std::variant<std::uint64_t, std::double_t, std::string, bool, EntityDescriptor>;

    using ValueNode = std::unordered_map<std::string, PrimitiveFieldValue>;

    struct ViewValue {
        ViewPath path;
        PrimitiveFieldValue value;
    };

    struct View {
        EntityDescriptor root;
        std::vector<ViewValue> values;
    };

    struct EventEntity {
        EntityDescriptor descriptor;
        ValueNode node;
    };

    bool operator==(const EventEntity &lhs, const EventEntity &rhs) {
        return lhs.descriptor == rhs.descriptor && lhs.node == rhs.node;
    }

    struct Event {
        EventID id;
        EventEntity entity;
    };

    bool operator==(const Event &lhs, const Event &rhs) {
        return lhs.id == rhs.id && lhs.entity == rhs.entity;
    }

    using ViewCallback =  void(EventID, View);

    struct ViewReader {
        virtual View read_view(ViewDescriptor descriptor) = 0;

        virtual void register_view_callback(ViewTypeID view_type, ViewCallback callback) = 0;
    };


    using EventReceiver = std::function<nonstd::expected<void, std::string>(Event evt)>;

    /*
    enum class PrimitiveFieldType {
        Integer,
        Float,
        String,
        Boolean,
        Reference,
    };
    */

    /*
   struct ViewSchemaNode {
       std::vector<std::string> field_names;
       std::vector<ViewSchemaNode> field_paths;
   };

   bool operator==(const ViewSchemaNode &lhs, const ViewSchemaNode &rhs) {
       return lhs.field_names == rhs.field_names && lhs.field_paths == rhs.field_paths;
   }
    */

    /*
    struct ViewSchema {
        ViewTypeID type;
        ViewSchemaNode structure;
    };


    bool operator==(const ViewSchema &lhs, const ViewSchema &rhs) {
        return lhs.type == rhs.type && lhs.structure == rhs.structure;
    }
     */

    /*
    struct EntitySchemaNode {
        std::unordered_map<std::string, PrimitiveFieldType> primitives;
        std::unordered_map<std::string, EntitySchemaNode> objects;
    };

    bool operator==(const EntitySchemaNode &lhs, const EntitySchemaNode &rhs) {
        return lhs.primitives == rhs.primitives && lhs.objects == rhs.objects;
    }
     */

    /*
    struct EntitySchema {
        EntityTypeID type;
        EntitySchemaNode structure;
    };

    bool operator==(const EntitySchema &lhs, const EntitySchema &rhs) {
        return lhs.type == rhs.type && lhs.structure == rhs.structure;
    }
    */

    /*
    struct SchemaRegistry {
        virtual EntityTypeID register_entity_schema(EntitySchema schema) = 0;

        virtual ViewSchema register_view_schema(ViewSchema schema) = 0;
    };
     */
}

namespace std {

    template<>
    struct hash<eventview::EntityDescriptor> {
        std::size_t operator()(const eventview::EntityDescriptor &ed) const {
            using std::size_t;
            using std::hash;
            using std::uint64_t;
            return (hash<uint64_t>()(ed.id) ^ (hash<uint64_t>()(ed.type) << 1ull));
        }
    };


    template<>
    struct hash<eventview::PathElement> {
        std::size_t operator()(const eventview::PathElement &pe) const {
            using std::size_t;
            using std::hash;
            using std::uint64_t;

            return (((hash<string>()(pe.name) ^ hash<uint64_t >()(pe.type) << 1ull) >> 1ull) ^ hash<bool >()(pe.forward) << 1ull);
        }
    };
}


#endif //EVENTVIEW_EVENTVIEW_H