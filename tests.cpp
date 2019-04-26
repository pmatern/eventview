#include <tuple>
#include <string>
#include <functional>
#include <variant>

#include "eventview.h"
#include "snowflake.h"
#include "eventlog.h"
#include "eventwriter.h"
#include "entitystorage.h"
#include "publish.h"
#include "expected.h"
#include "query.h"

#define CATCH_CONFIG_MAIN

#include "catch.h"

using namespace eventview;

TEST_CASE("value node") {

    SnowflakeProvider sp{ 125 };

    ValueNode child{
            {{"name", std::string{"jane"}}, {"age", std::uint64_t{12}}}
    };
    ValueNode node{
            {{"name",     std::string{"john"}}, {"age", std::uint64_t{41}}, {"department_id", EntityDescriptor{sp.next(), 5}}}
    };

    REQUIRE(node.size() == 3);

    auto name = node.find("name");
    REQUIRE(name != node.end());

    auto name_val = name->second;

    REQUIRE(std::holds_alternative<std::string>(name_val));
    REQUIRE("john" == *std::get_if<std::string>(&name_val));

    auto age = node.find("age");
    REQUIRE(age != node.end());

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
            {{"name",     std::string{"john"}}, {"age", std::uint64_t{41}}}
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
            {{"name",     std::string{"john"}}, {"age", std::uint64_t{41}}}
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
            {{"name",     std::string{"john"}}, {"age", std::uint64_t{41}}, {"department_id", EntityDescriptor{3453, 5}}}
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
            {{"name",     std::string{"john"}}, {"age", 41ull}, {"department_id", dept_id}}
    };

    EntityDescriptor desc{sp.next(), 21};
    EventEntity entity{desc, node};

    auto write_time = sp.next();
    StorageNode sn{write_time, entity};

    REQUIRE(sn.exists());
    REQUIRE(sn.type() == desc.type);

    auto fields = sn.get_fields();

    auto name_val = fields.find("name");

    REQUIRE(name_val != fields.end());

    auto john = name_val->second;

    REQUIRE(std::holds_alternative<std::string>(john));
    REQUIRE("john" == *std::get_if<std::string>(&john));


    ValueNode replacement {
            {{"age", 91ull}}
    };

    //this write won't take effect
    EventEntity replace_entity{ desc, replacement};
    auto result = sn.update_fields(write_time - 100, replace_entity);

    REQUIRE(result.has_value());
    auto result_val = result.value();

    REQUIRE(result_val.size() == 0);

    auto unchanged_fields = sn.get_fields();

    auto unchanged_age_val = unchanged_fields.find("age");

    REQUIRE(unchanged_age_val != unchanged_fields.end());

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

    auto changed_age_val = changed_fields.find("age");

    REQUIRE(changed_age_val != changed_fields.end());

    auto ninety_one = changed_age_val->second;

    REQUIRE(std::holds_alternative<std::uint64_t>(ninety_one));
    REQUIRE(91ull == *std::get_if<std::uint64_t>(&ninety_one));

    sn.deref(sp.next());

    REQUIRE(!sn.exists());

}

TEST_CASE("entity store") {
    SnowflakeProvider sp{ 45 };
    EntityStore store{};

    ValueNode child{
            {{"name", std::string{"jane"}}, {"age", 12ull}}
    };

    EntityDescriptor dept_id{sp.next(), 5};

    ValueNode node{
            {{"name",     std::string{"john"}}, {"age", 41ull}, {"department_id", dept_id}}
    };

    EntityDescriptor desc{sp.next(), 21};
    EventEntity entity{desc, node};

    auto result = store.get(desc);
    REQUIRE(!result.has_value());

    auto put_result = store.put(sp.next(), entity);
    REQUIRE(put_result);
    auto removed_refs = put_result.value();

    REQUIRE(removed_refs.size()==0);

    auto reput_result = store.put(sp.next(), entity);
    REQUIRE(reput_result);
    auto reremoved_refs = reput_result.value();

    REQUIRE(reremoved_refs.size()==1);

    auto& fields = store.get(desc);
    REQUIRE(fields);
    auto& fields_val = fields->get().get_fields();

    REQUIRE(fields_val == node);

}

TEST_CASE("publish round trip") {
    SnowflakeProvider sp{ 68 };
    std::shared_ptr<EntityStore> store = std::make_shared<EntityStore>();
    Publisher pub{store};
    EntityDescriptor mgr_ref = EntityDescriptor{sp.next(), 90ull};

    ValueNode node1{
            {{"name", std::string{"jane"}}, {"age", std::uint64_t{12}}}
    };

    ValueNode node2{
            {{"name",   std::string{"john"}}, {"age", std::uint64_t{41}}, {"manager_id", mgr_ref}}
    };

    EntityDescriptor desc1{sp.next(), 21};
    EventEntity entity1{desc1, node1};

    EntityDescriptor desc2{sp.next(), 21};
    EventEntity entity2{desc2, node2};

    Event sent1{sp.next(), entity1};
    Event sent2{sp.next(), entity2};

    auto result1 = pub.publish(sent1);
    REQUIRE(result1);

    auto result2 = pub.publish(sent2);
    REQUIRE(result2);


    auto& lookup1 = store->get(desc1);
    REQUIRE(lookup1);
    auto& found1 = lookup1->get();

    REQUIRE(found1.get_fields() == node1);


    auto& lookup2 = store->get(desc2);
    REQUIRE(lookup2);
    auto& found2 = lookup2->get();

    REQUIRE(found2.get_fields() == node2);

    auto& stub = store->get(mgr_ref);
    REQUIRE(stub);
    auto& found_stub = stub->get();

    auto& stub_referencers = found_stub.referencers_for_field("manager_id");
    REQUIRE(stub_referencers);

    auto& referencer = stub_referencers.value();

    REQUIRE(referencer.size() == 1);
    REQUIRE(referencer[0] == desc2);

    //TODO test stub referencers removed
}


TEST_CASE("event writer round trip") {
    std::shared_ptr<EntityStore> store = std::make_shared<EntityStore>();
    Publisher pub{store};

    EventWriter writer{ 46, [&](Event evt) {
        return pub.publish(evt);
    }};

    EntityDescriptor mgr_ref{writer.next_id(), 90ull};


    ValueNode node{
            {{"name",   std::string{"john"}}, {"age", std::uint64_t{41}}, {"manager_id", mgr_ref}}
    };

    EntityDescriptor desc{writer.next_id(), 21};
    EventEntity entity{desc, node};

    auto result = writer.write_event(entity);
    REQUIRE(result);

    auto& lookup = store->get(desc);
    REQUIRE(lookup);
    auto& found = lookup->get();

    REQUIRE(found.get_fields() == node);
}

TEST_CASE("write to query loop") {
    std::shared_ptr<EntityStore> store = std::make_shared<EntityStore>();
    Publisher pub{store};
    ViewProcessor view_processor{store};

    EventWriter writer{406, [&](Event evt) {
        return pub.publish(evt);
    }};

    EntityDescriptor manager_desc{writer.next_id(), 23};

    ValueNode node{
        {{"name", std::string{"john"}}, {"age", std::uint64_t{41}}, {"manager_id", manager_desc}}
    };

    ValueNode mgr_node{
        {{"name", std::string{"ted"}}, {"age", std::uint64_t{56}}}
    };

    EntityDescriptor desc{writer.next_id(), 21};
    EventEntity entity{desc, node};
    EventEntity manager_entity{manager_desc, mgr_node};

    auto mgr_result = writer.write_event(manager_entity);
    REQUIRE(mgr_result);
    auto result = writer.write_event(entity);
    REQUIRE(result);

    ViewDescriptor view_desc{manager_desc, {}};
    ViewPath vp_1{};
    vp_1.push_back({"name", 0, false});

    ViewPath vp_2{};
    vp_2.push_back({"age", 0, false});

    ViewPath vp_3{};
    vp_3.push_back({"manager_id", 21, false});
    vp_3.push_back({"name", 0, false});

    view_desc.paths.push_back(vp_1);
    view_desc.paths.push_back(vp_2);
    view_desc.paths.push_back(vp_3);

    const auto &view = view_processor.query(view_desc);

    REQUIRE(view);
    REQUIRE(view->root == view_desc.root);
    REQUIRE(view->values.size() == view_desc.paths.size());

    REQUIRE(*std::get_if<std::string>(&view->values[0].value) == "ted");
    REQUIRE(*std::get_if<std::uint64_t>(&view->values[1].value) == 56);
    REQUIRE(*std::get_if<std::string>(&view->values[2].value) =="john");
}