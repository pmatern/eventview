#include <tuple>
#include <string>
#include <functional>
#include <variant>

#include "eventview.h"
#include "snowflake.h"
#include "eventlog.h"
#include "eventwriter.h"
#include "entitystorage.h"
#include "expected.h"

#define CATCH_CONFIG_MAIN

#include "catch.h"

using namespace eventview;

TEST_CASE("value node") {

    SnowflakeProvider sp{ 125 };

    ValueNode child{
            {{"name", std::string{"jane"}}, {"age", std::uint64_t{12}}}
    };
    ValueNode node{
            {{"name",     std::string{"john"}}, {"age", std::uint64_t{41}}, {"department_id", EntityDescriptor{sp.next(), 5}}},
            {{"children", child}}
    };

    REQUIRE(node.fields.size() == 3);

    auto name = node.fields.find("name");
    REQUIRE(name != node.fields.end());

    auto name_val = name->second;

    REQUIRE(std::holds_alternative<std::string>(name_val));
    REQUIRE("john" == *std::get_if<std::string>(&name_val));

    auto age = node.fields.find("age");
    REQUIRE(age != node.fields.end());

    auto age_val = age->second;

    REQUIRE(std::holds_alternative<std::uint64_t>(age_val));
    REQUIRE(41ull == *std::get_if<std::uint64_t>(&age_val));


}

TEST_CASE("Test IDPacker") {
    SnowflakeIDPacker packer{};
    auto packed = packer.pack(345, 45, 2);
    auto unpacked = packer.unpack(packed);

    REQUIRE(std::get<0>(unpacked) == 345);
    REQUIRE(std::get<1>(unpacked) == 45);
    REQUIRE(std::get<2>(unpacked) == 2);
}

TEST_CASE("Generate snowflakes") {
    SnowflakeProvider provider{7};
    auto first = provider.next();
    auto second = provider.next();
    auto third = provider.next();

    REQUIRE(second > first);
    REQUIRE(third > second);

}

TEST_CASE("log event") {
    Event received;
    eventview::EventReceiver rcvr = [&](Event &&evt) -> nonstd::expected<void, std::string> {
        received = std::move(evt);
        return {};
    };

    EventLog el{rcvr};

    ValueNode child{
            {{"name", std::string{"jane"}}, {"age", std::uint64_t{12}}}
    };
    ValueNode node{
            {{"name",     std::string{"john"}}, {"age", std::uint64_t{41}}},
            {{"children", child}}
    };

    EntityDescriptor desc{577, 21};
    EventEntity entity{desc, node};
    Event sent{848467, entity};

    Event to_send{sent};
    auto result = el.append(std::move(to_send));
    REQUIRE(result);
    REQUIRE(received == sent);
}

TEST_CASE("write event") {
    Event received;
    eventview::EventReceiver rcvr = [&](Event &&evt) -> nonstd::expected<void, std::string> {
        received = std::move(evt);
        return {};
    };

    EventLog el{rcvr};
    EventWriter writer{554, rcvr};

    ValueNode child{
            {{"name", std::string{"jane"}}, {"age", std::uint64_t{12}}}
    };
    ValueNode node{
            {{"name",     std::string{"john"}}, {"age", std::uint64_t{41}}},
            {{"children", child}}
    };

    EntityDescriptor desc{577, 21};
    EventEntity entity{desc, node};

    auto result = writer.write_event(entity);
    REQUIRE(result);
    REQUIRE(received.entity == entity);
}

TEST_CASE("storage node referencers") {
    ValueNode child{
            {{"name", std::string{"jane"}}, {"age", std::uint64_t{12}}}
    };
    ValueNode node{
            {{"name",     std::string{"john"}}, {"age", std::uint64_t{41}}, {"department_id", EntityDescriptor{3453, 5}}},
            {{"children", child}}
    };

    EntityDescriptor desc{577, 21};
    EventEntity entity{desc, node};

    StorageNode sn{34234, entity};

    REQUIRE(sn.exists());

    SnowflakeProvider sp{ 56 };

    EventID write_time = sp.next();
    EntityDescriptor referencer = {write_time, 436};

    sn.add_referencer(sp.next(), "manager", referencer);

    auto refs = sn.referencers_for_field("manager");

    REQUIRE(refs.has_value());

    auto refs_val = refs.value();
    REQUIRE(refs_val.size() == 1);

    REQUIRE(referencer == refs_val[0]);


    sn.remove_referencer(write_time - 1, "manager", referencer);

    auto refs_unchanged = sn.referencers_for_field("manager");

    REQUIRE(refs_unchanged.has_value());

    auto refs_unchanged_val = refs.value();
    REQUIRE(refs_unchanged_val.size() == 1);

    REQUIRE(referencer == refs_unchanged_val[0]);

    sn.remove_referencer(sp.next(), "manager", referencer);

    auto refs_changed = sn.referencers_for_field("manager");

    REQUIRE(refs_changed.has_value());

    auto refs_changed_val = refs_changed.value();
    REQUIRE(refs_changed_val.size() == 0);

}

TEST_CASE("storage node fields") {
    SnowflakeProvider sp{ 85 };

    ValueNode child{
            {{"name", std::string{"jane"}}, {"age", 12ull}}
    };

    EntityDescriptor dept_id{sp.next(), 5};

    ValueNode node{
            {{"name",     std::string{"john"}}, {"age", 41ull}, {"department_id", dept_id}},
            {{"children", child}}
    };

    EntityDescriptor desc{sp.next(), 21};
    EventEntity entity{desc, node};

    auto write_time = sp.next();
    StorageNode sn{write_time, entity};

    REQUIRE(sn.exists());
    REQUIRE(sn.type() == desc.type);

    //const nonstd::expected<std::reference_wrapper<const std::unordered_map<std::string, PrimitiveFieldValue> >, std::string>
    auto fields = sn.get_fields();

    REQUIRE(fields.has_value());

    auto fields_map = fields.value().get();
    auto name_val = fields_map.find("name");

    REQUIRE(name_val != fields_map.end());

    auto john = name_val->second;

    REQUIRE(std::holds_alternative<std::string>(john));
    REQUIRE("john" == *std::get_if<std::string>(&john));


    ValueNode replacement {
            {{"age", 91ull}},
            {}
    };

    //this write won't take effect
    EventEntity replace_entity{ desc, replacement};
    auto result = sn.update_fields(write_time - 100, replace_entity);

    REQUIRE(result.has_value());
    auto result_val = result.value();

    REQUIRE(result_val.size() == 0);

    auto unchanged_fields = sn.get_fields();

    REQUIRE(unchanged_fields.has_value());

    auto unchanged_fields_map = unchanged_fields.value().get();
    auto unchanged_age_val = unchanged_fields_map.find("age");

    REQUIRE(unchanged_age_val != unchanged_fields_map.end());

    auto forty_one = unchanged_age_val->second;

    REQUIRE(std::holds_alternative<std::uint64_t>(forty_one));
    REQUIRE(41ull == *std::get_if<std::uint64_t>(&forty_one));


    //this write WILL take effect
    auto real_result = sn.update_fields(sp.next(), replace_entity);

    REQUIRE(real_result.has_value());
    auto real_result_val = real_result.value();

    REQUIRE(real_result_val.size() == 1);
    REQUIRE(real_result_val["department_id"] == dept_id);

    auto changed_fields = sn.get_fields();

    REQUIRE(changed_fields.has_value());

    auto changed_fields_map = changed_fields.value().get();
    auto changed_age_val = changed_fields_map.find("age");

    REQUIRE(changed_age_val != changed_fields_map.end());

    auto ninety_one = changed_age_val->second;

    REQUIRE(std::holds_alternative<std::uint64_t>(ninety_one));
    REQUIRE(91ull == *std::get_if<std::uint64_t>(&ninety_one));

    sn.deref(sp.next());

    REQUIRE(!sn.exists());

    auto non_exist_result = sn.get_fields();
    REQUIRE(!non_exist_result);

}
