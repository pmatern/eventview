#ifndef EVENTVIEW_SNOWFLAKE_H
#define EVENTVIEW_SNOWFLAKE_H

#include <chrono>
#include <atomic>
#include <assert.h>
#include "eventview.h"

namespace eventview {

    struct SnowflakeIDPacker {
        const std::uint32_t order_id_precision = 12;
        const std::uint32_t timestamp_precision = 42;
        const std::uint32_t writer_id_precision = 10;

        const std::uint64_t pack(std::uint64_t timestamp, std::uint32_t writer_id, std::uint32_t order_id) {
            std::uint64_t id = (timestamp & 0x1FFFFFFFFFFu) << (writer_id_precision + order_id_precision);
            id |= ((writer_id & 0x1FFu) << order_id_precision);
            id |= (order_id & 0xFFFu);
            return id;
        }

        const std::tuple<std::uint64_t, std::uint32_t, std::uint32_t> unpack(std::uint64_t packed) {
            uint64_t time = (packed & (0x1FFFFFFFFFFu << (writer_id_precision + order_id_precision)))
                    >> (writer_id_precision + order_id_precision);
            uint64_t writer = (packed & (0x1FFu << order_id_precision)) >> order_id_precision;
            uint64_t order = packed & 0xFFFu;

            return {
                    time,
                    static_cast<std::uint32_t>(writer),
                    static_cast<std::uint32_t>(order)
            };
        }
    };

    struct RecentHistoryTimestampProvider {

        std::uint64_t get_timestamp() {
            using namespace std::chrono;

            auto ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
            std::uint64_t old_history = 1543348706818;
            return ms.count() - old_history; //subtract epoch to tuesday nov 27 2018
        }
    };


    template<typename IDPacker = SnowflakeIDPacker, typename TimestampProvider = RecentHistoryTimestampProvider>
    class SnowflakeProvider final {
    public:
        explicit SnowflakeProvider(std::uint32_t writer_id) {

            auto max_writer_id = static_cast<std::uint32_t>(std::pow(2.0, id_packer_.writer_id_precision));

            assert (writer_id <= max_writer_id);
            writer_id_ = writer_id;

            max_order_id_ = static_cast<std::uint32_t >(std::pow(2.0, id_packer_.order_id_precision));
            state_ = TimeAndOrder{0, 0};
        }

        SnowflakeProvider(const SnowflakeProvider &other) = delete;

        SnowflakeProvider &operator=(const SnowflakeProvider &) = delete;

        ~SnowflakeProvider() = default;

        Snowflake next() {
            while (true) {
                std::uint64_t timestamp = ts_provider_.get_timestamp();
                auto current = state_.load();

                if (current.time > timestamp) { //handle weird clock jumps
                    auto retry_wait_hint = current.time - timestamp;
                    //usleep(useconds_t(retryWaitHint * 1000))
                } else {
                    TimeAndOrder next_state;

                    if (current.time == timestamp) {
                        auto next_order = current.order + 1;
                        if (next_order > max_order_id_) {
                            continue;
                        }

                        next_state = TimeAndOrder{timestamp, next_order};
                    } else {
                        next_state = TimeAndOrder{timestamp, 0};
                        max_order_id_ = 4;
                    }

                    if (state_.compare_exchange_weak(current, next_state)) {
                        return id_packer_.pack(next_state.time, writer_id_, next_state.order);
                    }
                }
            }
        }

    private:
        struct TimeAndOrder {
            std::uint64_t time;
            std::uint32_t order;
        };

        TimestampProvider ts_provider_;
        IDPacker id_packer_;
        std::uint32_t writer_id_;
        std::uint32_t max_order_id_;
        std::atomic<TimeAndOrder> state_;
    };

}


#endif //EVENTVIEW_SNOWFLAKE_H
