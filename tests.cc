#include <tuple>
#include <string>
#include <functional>
#include <variant>
#include <atomic>

#include "types.h"
#include "snowflake.h"
#include "eventlog.h"
#include "eventwriter.h"
#include "entitystorage.h"
#include "publish.h"
#include "viewimpl.h"
#include "publishimpl.h"
#include "mpsc.h"
#include "opdispatch.h"
#include "eventview.h"

#define CATCH_CONFIG_MAIN

#include "catch.h"

using namespace eventview;

TEST_CASE("value node") {

    SnowflakeProvider sp{ 125 };

    Entity::Fields node{
            {{"name",  {std::string{"john"}}}, {"age", {41ull}}, {"department_id", {EntityDescriptor{sp.next(), 5}}}}
    };

    REQUIRE(node.size() == 3);

    auto name = node.find("name");
    REQUIRE(name != node.end());

    auto name_val = name->second;

    REQUIRE(name_val.is_string());
    REQUIRE("john" == name_val.as_string());

    auto age = node.find("age");
    REQUIRE(age != node.end());

    auto age_val = age->second;

    REQUIRE(age_val.is_long());
    REQUIRE(41ull == age_val.as_long());


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
    eventview::EventReceiver rcvr = [&](Event &&evt) -> void {
        received = std::move(evt);
        return {};
    };

    EventLog el{rcvr};

    EntityDescriptor desc{577, 21};
    Entity entity{desc.id, desc.type};
    entity.set_field("name", {std::string{"john"}});
    entity.set_field("age", {41ull});


    Event sent{848467, entity};

    Event to_send{sent};
    el.append(std::move(to_send));
    REQUIRE(received == sent);
}

TEST_CASE("write event") {
    Event received;
    eventview::EventReceiver rcvr = [&](Event &&evt) -> void {
        received = std::move(evt);
        return {};
    };

    EventLog el{rcvr};
    EventWriter writer{554, rcvr};

    EntityDescriptor desc{577, 21};
    Entity entity{desc.id, desc.type};
    entity.set_field("name", {std::string{"john"}});
    entity.set_field("age", {41ull});


    auto result = writer.write_event(entity);
    REQUIRE(result);
    REQUIRE(entity == received.entity);
}

TEST_CASE("storage node referencers") {
    EntityDescriptor desc{577, 21};
    Entity entity{desc.id, desc.type};
    entity.set_field("name", {std::string{"john"}});
    entity.set_field("age", {41ull});
    entity.set_field("department_id", {EntityDescriptor{3453, 5}});

    StorageNode sn{34234, entity};

    REQUIRE(sn.exists());

    SnowflakeProvider sp{ 56 };

    EventID write_time = sp.next();
    EntityDescriptor referencer = {write_time, 436};

    sn.add_referencer(sp.next(), "manager", referencer);

    auto refs_val = sn.referencers_for_field("manager");


    REQUIRE(refs_val.size() == 1);

    REQUIRE(referencer == refs_val[0]);


    sn.remove_referencer(write_time - 1, "manager", referencer);

    auto refs_unchanged_val = sn.referencers_for_field("manager");

    REQUIRE(refs_unchanged_val.size() == 1);

    REQUIRE(referencer == refs_unchanged_val[0]);

    sn.remove_referencer(sp.next(), "manager", referencer);

    auto refs_changed_val = sn.referencers_for_field("manager");

    REQUIRE(refs_changed_val.size() == 0);

}

TEST_CASE("storage node fields") {
    SnowflakeProvider sp{ 85 };

    EntityDescriptor dept_id{sp.next(), 5};


    EntityDescriptor desc{sp.next(), 21};
    Entity entity{desc.id, desc.type};
    entity.set_field("name", {std::string{"john"}});
    entity.set_field("age", {41ull});
    entity.set_field("department_id", {dept_id});


    auto write_time = sp.next();
    StorageNode sn{write_time, entity};

    REQUIRE(sn.exists());
    REQUIRE(sn.type() == desc.type);

    auto fields = sn.get_fields();

    auto name_val = fields.find("name");

    REQUIRE(name_val != fields.end());

    auto john = name_val->second;

    REQUIRE(john.is_string());
    REQUIRE("john" == john.as_string());

    //this write won't take effect
    Entity replace_entity{ desc.id, desc.type};
    replace_entity.set_field("age", {91ull});

    auto result_val = sn.update_fields(write_time - 100, replace_entity);

    REQUIRE(result_val.size() == 0);

    auto unchanged_fields = sn.get_fields();

    auto unchanged_age_val = unchanged_fields.find("age");

    REQUIRE(unchanged_age_val != unchanged_fields.end());

    auto forty_one = unchanged_age_val->second;

    REQUIRE(forty_one.is_long());
    REQUIRE(41ull == forty_one.as_long());


    //this write WILL take effect
    auto real_result_val = sn.update_fields(sp.next(), replace_entity);

    REQUIRE(real_result_val.size() == 1);
    REQUIRE(real_result_val["department_id"] == dept_id);

    auto changed_fields = sn.get_fields();

    auto changed_age_val = changed_fields.find("age");

    REQUIRE(changed_age_val != changed_fields.end());

    auto ninety_one = changed_age_val->second;

    REQUIRE(ninety_one.is_long());
    REQUIRE(91ull == ninety_one.as_long());

    sn.deref(sp.next());

    REQUIRE(!sn.exists());

}

TEST_CASE("entity store") {
    SnowflakeProvider sp{ 45 };
    EntityStore store{};

    EntityDescriptor dept_id{sp.next(), 5};

    Entity::Fields node{
        {{"name",  {std::string{"john"}}}, {"age", {41ull}}, {"department_id", {dept_id}}}
    };

    EntityDescriptor desc{sp.next(), 21};
    Entity entity{desc.id, desc.type};
    entity.set_field("name", {std::string{"john"}});
    entity.set_field("age", {41ull});
    entity.set_field("department_id", {dept_id});


    auto result = store.get(desc);
    REQUIRE(!result.has_value());

    auto removed_refs = store.put(sp.next(), entity);

    REQUIRE(removed_refs.size()==0);

    auto reremoved_refs = store.put(sp.next(), entity);

    REQUIRE(reremoved_refs.size()==1);

    const auto& fields = store.get(desc);
    REQUIRE(fields);
    auto& fields_val = fields->get().get_fields();

    REQUIRE(fields_val == node);

}

TEST_CASE("publish round trip") {
    SnowflakeProvider sp{ 68 };
    std::shared_ptr<EntityStore> store = std::make_shared<EntityStore>();
    PublisherImpl pub{store};
    EntityDescriptor mgr_ref = EntityDescriptor{sp.next(), 90ull};

    Entity::Fields node1{
        {{"name", {std::string{"jane"}}}, {"age", {12ull}}}
    };

    Entity::Fields node2{
        {{"name", {std::string{"john"}}}, {"age", {41ull}}, {"manager_id", {mgr_ref}}}
    };

    EntityDescriptor desc1{sp.next(), 21};
    Entity entity1{desc1.id, desc1.type};
    entity1.set_field("name", {std::string{"jane"}});
    entity1.set_field("age", {12ull});

    EntityDescriptor desc2{sp.next(), 21};
    Entity entity2{desc2.id, desc2.type};
    entity2.set_field("name", {std::string{"john"}});
    entity2.set_field("age", {41ull});
    entity2.set_field("manager_id", {mgr_ref});



    Event sent1{sp.next(), entity1};
    Event sent2{sp.next(), entity2};

    pub.publish(Event{sent1});

    pub.publish(Event{sent2});

    const auto& lookup1 = store->get(desc1);
    REQUIRE(lookup1);
    auto& found1 = lookup1->get();

    REQUIRE(found1.get_fields() == node1);


    const auto& lookup2 = store->get(desc2);
    REQUIRE(lookup2);
    auto& found2 = lookup2->get();

    REQUIRE(found2.get_fields() == node2);

    const auto& stub = store->get(mgr_ref);
    REQUIRE(stub);
    auto& found_stub = stub->get();

    auto referencer = found_stub.referencers_for_field("manager_id");

    REQUIRE(referencer.size() == 1);
    REQUIRE(referencer[0] == desc2);

    //TODO test stub referencers removed
}


TEST_CASE("event writer round trip") {
    std::shared_ptr<EntityStore> store = std::make_shared<EntityStore>();
    PublisherImpl pub{store};

    EventWriter writer{ 46, [&](Event &&evt) {
        pub.publish(std::move(evt));
    }};

    EntityDescriptor mgr_ref{writer.next_id(), 90ull};


    Entity::Fields node {
        {{"name", {std::string{"john"}}}, {"age", {41ull}}, {"manager_id", {mgr_ref}}}
    };

    EntityDescriptor desc{writer.next_id(), 21};
    Entity entity{desc.id, desc.type};
    entity.set_field("name", {std::string{"john"}});
    entity.set_field("age", {41ull});
    entity.set_field("manager_id", {mgr_ref});


    auto result = writer.write_event(entity);
    REQUIRE(result);

    const auto& lookup = store->get(desc);
    REQUIRE(lookup);
    auto& found = lookup->get();

    REQUIRE(found.get_fields() == node);
}

TEST_CASE("write to read_view loop") {
    std::shared_ptr<EntityStore> store = std::make_shared<EntityStore>();
    PublisherImpl pub{store};
    ViewReaderImpl view_processor{store};

    EventWriter writer{406, [&](Event &&evt) {
        pub.publish(std::move(evt));
    }};

    EntityDescriptor manager_desc{writer.next_id(), 23};

    EntityDescriptor desc{writer.next_id(), 21};
    Entity entity{desc.id, desc.type};
    entity.set_field("name", {std::string{"john"}});
    entity.set_field("age", {41ull});
    entity.set_field("manager_id", {manager_desc});


    Entity manager_entity{manager_desc.id, manager_desc.type};
    manager_entity.set_field("name", {std::string{"ted"}});
    manager_entity.set_field("age", {56ull});

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

    const auto &view = view_processor.read_view(view_desc);

    REQUIRE(view);

    const auto& name_val = view->get_path_val<1>({"name"});
    REQUIRE(name_val);
    REQUIRE(name_val->is_string());
    REQUIRE(name_val->as_string() == "ted");

    const auto& age_val = view->get_path_val<1>({"age"});
    REQUIRE(age_val);
    REQUIRE(age_val->is_long());
    REQUIRE(age_val->as_long() == 56ull);

    const auto& employee_name_val = view->get_path_val<2>({"manager_id", "name"});
    REQUIRE(employee_name_val);
    REQUIRE(employee_name_val->is_string());
    REQUIRE(employee_name_val->as_string() == "john");

}

TEST_CASE("basic mpsc") {
    MPSC<std::uint64_t, 5> mpsc{};

    REQUIRE(!mpsc.consume());

    REQUIRE(mpsc.produce(45ull));

    auto got = mpsc.consume();

    REQUIRE(got);

    REQUIRE(*got == 45ull);

    //queue size of 5 is actually only 4 usable slots right now :(
    for (int i=0; i<4; ++i) {
        REQUIRE(mpsc.produce(i));
    }

    REQUIRE(!mpsc.produce(6));

    for (int i=0; i<4; ++i) {
        auto consumed = mpsc.consume();
        REQUIRE(consumed);
        REQUIRE(i == *consumed);
    }
}

View build_view() {
    ViewBuilder vb{{2324, 43}};

    ViewPath vp_1{};
    vp_1.push_back({"name", 0, false});
    vb.add_path_val(vp_1, {std::string{"ted"}});

    ViewPath vp_2{};
    vp_2.push_back({"age", 0, false});
    vb.add_path_val(vp_2, {67ull});

    ViewPath vp_3{};
    vp_3.push_back({"manager_id", 21, false});
    vp_3.push_back({"name", 0, false});
    vb.add_path_val(vp_3,  {std::string{"jack"}});

    return *vb.finish();
}

TEST_CASE("basic opdispatch") {

    EventPublishCallback pub = [](Event &&evt){
        std::cout << "event accepted" << std::endl;
    };

    ViewReadCallback view = [](const ViewDescriptor &view_desc) -> const std::optional<View> {
        std::cout << "returning view" << std::endl;
        return build_view();
    };

    OpDispatch<5> dispatch{pub, view};

    EntityDescriptor desc1{234, 21};
    Entity entity1{desc1.id, desc1.type};
    entity1.set_field("name", {std::string{"jane"}});
    entity1.set_field("age", {12ull});

    auto pub_f = dispatch.publish_event(Event{678,  entity1});
    pub_f.get();

    std::cout << "write complete" << std::endl;

    auto read_f = dispatch.read_view(ViewDescriptor{});
    auto res = read_f.get();

    std::cout << "view read complete" << std::endl;

    REQUIRE(res);

    auto compare = build_view();

    REQUIRE(compare.get_path_val<1>({"name"}) == res->get_path_val<1>({"name"}));
    REQUIRE(compare.get_path_val<1>({"age"}) == res->get_path_val<1>({"age"}));
    REQUIRE(compare.get_path_val<2>({"manager_id", "name"}) == res->get_path_val<2>({"manager_id", "name"}));

}

TEST_CASE("eventview factory") {
    auto system =  make_eventview_system<5>();

    auto& publisher = system.first;
    auto& reader = system.second;

    auto writer = make_writer<5>(475, publisher);


    EntityDescriptor manager_desc{writer.next_id(), 23};

    EntityDescriptor desc{writer.next_id(), 21};
    Entity entity{desc};
    entity.set_field("name", {std::string{"john"}});
    entity.set_field("age", {41ull});
    entity.set_field("manager_id", {manager_desc});


    Entity manager_entity{manager_desc};
    manager_entity.set_field("name", {std::string{"ted"}});
    manager_entity.set_field("age", {56ull});

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

    const auto &view = reader.read_view(view_desc);

    REQUIRE(view);

    const auto& name_val = view->get_path_val<1>({"name"});
    REQUIRE(name_val);
    REQUIRE(name_val->is_string());
    REQUIRE(name_val->as_string() == "ted");

    const auto& age_val = view->get_path_val<1>({"age"});
    REQUIRE(age_val);
    REQUIRE(age_val->is_long());
    REQUIRE(age_val->as_long() == 56ull);

    const auto& employee_name_val = view->get_path_val<2>({"manager_id", "name"});
    REQUIRE(employee_name_val);
    REQUIRE(employee_name_val->is_string());
    REQUIRE(employee_name_val->as_string() == "john");
}


TEST_CASE("eventwriter no receiver") {
    auto writer = make_writer(23);

    EntityDescriptor manager_desc{writer.next_id(), 23};

    EntityDescriptor desc{writer.next_id(), 21};
    Entity entity{desc.id, desc.type};
    entity.set_field("name", {std::string{"john"}});
    entity.set_field("age", {41ull});
    entity.set_field("manager_id", {manager_desc});


    Entity manager_entity{manager_desc.id, manager_desc.type};
    manager_entity.set_field("name", {std::string{"ted"}});
    manager_entity.set_field("age", {56ull});


    auto mgr_result = writer.write_event(manager_entity);
    REQUIRE(mgr_result);
    auto result = writer.write_event(entity);
    REQUIRE(result);

}

TEST_CASE("expected entity") {
    auto system =  make_eventview_system<5>();
    auto& publisher = system.first;
    auto& reader = system.second;
    auto writer = make_writer<5>(475, publisher);

    EntityDescriptor manager_desc{writer.next_id(), 23};

    EntityDescriptor desc{writer.next_id(), 21};
    Entity entity{desc};
    entity.set_field("name", {std::string{"john"}});
    entity.set_field("age", {41ull});
    entity.set_field("manager_id", {manager_desc});

    Entity manager_entity{manager_desc};
    manager_entity.set_field("name", {std::string{"ted"}});
    manager_entity.set_field("age", {56ull});

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

    view_desc.expectation = {{1,2}, result.event_id()};

    const auto &view = reader.read_view(view_desc);

    REQUIRE(!view);

    view_desc.expectation = {view_desc.root, result.event_id() + 100};

    const auto &view1 = reader.read_view(view_desc);

    REQUIRE(!view1);

    view_desc.expectation = {view_desc.root, result.event_id()};

    const auto &view2 = reader.read_view(view_desc);

    REQUIRE(view2);
}


TEST_CASE("write  and read api") {
    auto system =  make_eventview_system<5>();

    auto& publisher = system.first;
    auto& reader = system.second;

    auto writer = make_writer<5>(475, publisher);


    EntityDescriptor manager_desc{writer.next_id(), 23};

    EntityDescriptor desc{writer.next_id(), 21};
    Entity entity{desc};
    entity.set_field("name", {std::string{"john"}});
    entity.set_field("age", {41ull});
    entity.set_field("manager_id", {manager_desc});


    Entity manager_entity{manager_desc};
    manager_entity.set_field("name", {std::string{"ted"}});
    manager_entity.set_field("age", {56ull});

//    auto mgr_result = writer.write_event(manager_entity);
//    REQUIRE(mgr_result);
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

    auto res_and_view = write_and_read<5>(writer, manager_entity, reader, view_desc);

    const auto &view = res_and_view.view();

    REQUIRE(res_and_view.result());
    REQUIRE(view);

    const auto& name_val = view->get_path_val<1>({"name"});
    REQUIRE(name_val);
    REQUIRE(name_val->is_string());
    REQUIRE(name_val->as_string() == "ted");

    const auto& age_val = view->get_path_val<1>({"age"});
    REQUIRE(age_val);
    REQUIRE(age_val->is_long());
    REQUIRE(age_val->as_long() == 56ull);

    const auto& employee_name_val = view->get_path_val<2>({"manager_id", "name"});
    REQUIRE(employee_name_val);
    REQUIRE(employee_name_val->is_string());
    REQUIRE(employee_name_val->as_string() == "john");
}