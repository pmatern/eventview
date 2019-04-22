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

TEST_CASE("entity storage") {
    ValueNode child{
            {{"name", "jane"}, {"age", 12ull}}
    };
    ValueNode node{
            {{"name",     "john"}, {"age", 41ull}},
            {{"children", child}}
    };

    EntityDescriptor desc{577, 21};
    EventEntity entity{desc, node};

    StorageNode sn{34234, std::move(entity)};
}
