#ifndef EVENTVIEW_TYPES_H
#define EVENTVIEW_TYPES_H

#include <cstdint>
#include <cmath>
#include <string>
#include <variant>
#include <unordered_map>
#include <vector>
#include <functional>
#include <optional>
#include <numeric>

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

    const std::string path_to_string(const ViewPath &path) {
        std::string str;
        for (int i=0; i<path.size(); ++i) {
            str.append(path[i].name);
            if (i<path.size()-1) {
                str.append(".");
            }
        }
        return std::move(str);
    }

    const bool path_has_multiple_values(const ViewPath &path) {
        for (auto& elem : path) {
            if (elem.is_reverse_ref()) {
                return true;
            }
        }

        return false;
    }


    struct ViewDescriptor {
        EntityDescriptor root;
        std::vector<ViewPath> paths;
    };

    struct PrimitiveFieldValue {
        std::variant<std::uint64_t, std::double_t, std::string, bool, EntityDescriptor> val;

        bool is_long() const {
            return std::holds_alternative<std::uint64_t>(val);
        }

        const std::uint64_t &as_long() const {
            return *std::get_if<std::uint64_t>(&val);
        }

        bool is_double() const {
            return std::holds_alternative<std::double_t>(val);
        }

        const std::double_t &as_double() const {
            return *std::get_if<std::double_t>(&val);
        }

        bool is_string() const {
            return std::holds_alternative<std::string>(val);
        }

        const std::string &as_string() const {
            return *std::get_if<std::string>(&val);
        }

        bool is_descriptor() const {
            return std::holds_alternative<EntityDescriptor>(val);
        }

        const EntityDescriptor &as_descriptor() const {
            return *std::get_if<EntityDescriptor>(&val);
        }

    };

    bool operator==(const PrimitiveFieldValue &lhs, const PrimitiveFieldValue &rhs) {
        return lhs.val == rhs.val;
    }

    class ViewBuilder;

    class View final {
    public:

        View(const View &)=default;
        View& operator=(const View &)=default;
        View(View &&)=default;
        View& operator=(View &&)=default;
        ~View()=default;

        const std::optional<PrimitiveFieldValue> get_path_val(const ViewPath &path) const {
            assert(path.size() > 0);

            auto found =  values_.find(path_to_string(path));
            if (found == values_.end()) {
                return {};
            } else {
                return found->second;
            }
        }

        const std::vector<PrimitiveFieldValue> get_path_vals(const ViewPath &path) const {
            assert(path.size() > 0);

            std::vector<PrimitiveFieldValue> vals{};
            auto founds = values_.equal_range(path_to_string(path));
            for (auto i = founds.first; i != founds.second; i++) {
                vals.push_back(i->second);
            }

            return std::move(vals);
        }

        template<std::size_t size>
        const std::optional<PrimitiveFieldValue> get_path_val(const std::array<std::string, size> &path) const {
            assert(path.size() > 0);

            auto found =  values_.find(path_str_(path));
            if (found == values_.end()) {
                return {};
            } else {
                return found->second;
            }
        }

        template<std::size_t size>
        const std::vector<PrimitiveFieldValue> get_path_vals(const std::array<std::string, size> &path) const {
            assert(path.size() > 0);

            std::vector<PrimitiveFieldValue> vals{};
            auto founds = values_.equal_range(path_str_(path));
            for (auto i = founds.first; i != founds.second; i++) {
                vals.push_back(i->second);
            }

            return std::move(vals);
        }

    private:

        template<std::size_t size>
        const std::string path_str_(const std::array<std::string, size> &path) const {

            std::string str;
            for (int i=0; i<path.size(); ++i) {
                str.append(path[i]);
                if (i<path.size()-1) {
                    str.append(".");
                }
            }
            return std::move(str);
        }


        friend class ViewBuilder;
        View(EntityID id, EntityTypeID type): descriptor_{id, type}{}

        EntityDescriptor descriptor_;
        std::unordered_multimap<std::string, PrimitiveFieldValue> values_;
    };


    class ViewBuilder final {
    public:
        explicit ViewBuilder(EntityID id, EntityTypeID type): view_{id, type}{}
        ViewBuilder(const ViewBuilder &)=default;
        ViewBuilder& operator=(const ViewBuilder &)=default;
        ViewBuilder(ViewBuilder &&)=default;
        ViewBuilder& operator=(ViewBuilder &&)=default;
        ~ViewBuilder()=default;

        void add_path_val(ViewPath path, PrimitiveFieldValue value) {
            view_.values_.insert({path_to_string(path), value});
        }

        View finish() {
            return std::move(view_);
        }
    private:

        View view_;
    };


    class Entity final {
    public:

        using Fields = std::unordered_map<std::string, PrimitiveFieldValue>;

        Entity() : descriptor_{0,0}{}
        explicit Entity(EntityDescriptor desc): descriptor_{desc}{}
        explicit Entity(EntityTypeID type): descriptor_{0, type}{}
        Entity(EntityID id, EntityTypeID type): descriptor_{id, type}{}
        Entity(const Entity &)=default;
        Entity& operator=(const Entity &)=default;
        Entity(Entity &&)=default;
        Entity& operator=(Entity &&)=default;
        ~Entity()=default;

        void set_field(const std::string field, PrimitiveFieldValue val) {
            fields_[field] = val;
        }

        const Fields& fields() const {
            return fields_;
        }

        const EntityDescriptor& descriptor() const {
            return descriptor_;
        }

        void replace(Fields fields) {
            fields_ = std::move(fields);
        }

        void set_entity_id(EntityID id) {
            descriptor_.id = id;
        }


    private:
        friend bool operator== (const Entity& lhs, const Entity& rhs);

        EntityDescriptor descriptor_;
        Fields fields_;
    };

    bool operator==(const Entity &lhs, const Entity &rhs) {
        return lhs.descriptor_ == rhs.descriptor_ && lhs.fields_ == rhs.fields_;
    }

    struct Event {
        EventID id;
        Entity entity;

        Event() = default;
        Event(Event &&) = default;
        Event& operator=(Event &&) = default;
        Event(const Event &) = default;
        Event& operator=(const Event &) = default;
        ~Event() = default;

    };

    bool operator==(const Event &lhs, const Event &rhs) {
        return lhs.id == rhs.id && lhs.entity == rhs.entity;
    }

    using ViewCallback = std::function<void(EventID, const View &)>;

    using EventReceiver = std::function<void(Event evt)>;

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

            return (((hash<string>()(pe.name) ^ hash<uint64_t>()(pe.type) << 1ull) >> 1ull) ^
                    hash<bool>()(pe.forward) << 1ull);
        }
    };
}


#endif //EVENTVIEW_TYPES_H