#include <tuple>
#include <string>

#include "eventview.h"
#include "snowflake.h"
#include "eventlog.h"
#include "eventwriter.h"
#include "entitystorage.h"
#include "expected.h"

#define CATCH_CONFIG_MAIN

#include "catch.h"

using namespace eventview;

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
            {{"name", "jane"}, {"age", 12ull}}
    };
    ValueNode node{
            {{"name",     "john"}, {"age", 41ull}},
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
            {{"name", "jane"}, {"age", 12ull}}
    };
    ValueNode node{
            {{"name",     "john"}, {"age", 41ull}},
            {{"children", child}}
    };

    EntityDescriptor desc{577, 21};
    EventEntity entity{desc, node};

    auto result = writer.write_event(entity);
    REQUIRE(result);
    REQUIRE(received.entity == entity);
}

TEST_CASE("storage node") {
    ValueNode child{
            {{"name", "jane"}, {"age", 12ull}}
    };
    ValueNode node{
            {{"name",     "john"}, {"age", 41ull}, {"department_id", EntityDescriptor{3453, 5}}},
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
