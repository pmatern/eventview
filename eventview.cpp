#include <iostream>
#include <tuple>

#include "eventview.h"
#include "snowflake.h"


using namespace eventview;

void build() {
    EntityDescriptor desc{34, 56};
    auto type = desc.type;
    auto id = desc.id;

    SnowflakeProvider provider{2};
    auto sf = provider.next();

    std::cout << "type: " << type << " id: " << id << " sf: " << sf << std::endl;

    SnowflakeIDPacker packer{};
    auto packed = packer.pack(345, 45, 2);
    auto unpacked = packer.unpack(packed);

    std::cout << "time " << std::get<0>(unpacked) << " writer " << std::get<1>(unpacked) << " order "
              << std::get<1>(unpacked) << std::endl;
}

