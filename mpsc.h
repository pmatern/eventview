
#ifndef EVENTVIEW_MPSC_H
#define EVENTVIEW_MPSC_H

#include <array>
#include <thread>
#include <atomic>
#include <optional>
#include "types.h"

namespace eventview {


    template<typename Elem, std::uint32_t BuffSize>
    class MPSC {

    public:
        using index = typename std::array<Elem, BuffSize>::size_type;

        MPSC() : write_idx_{0}, read_idx_{0}, max_read_idx_{0}, ring_buff_{} {}

        MPSC(const MPSC &) = delete;

        MPSC &operator=(const MPSC &) = delete;

        MPSC(MPSC &&) = default;

        MPSC &operator=(MPSC &&) = default;

        ~MPSC() = default;

        inline bool produce(Elem elem);

        inline std::optional<Elem> consume();

    private:

        inline index position(index count) {
            return (count % BuffSize);
        }

        std::atomic<index> write_idx_;
        std::atomic<index> read_idx_;
        std::atomic<index> max_read_idx_;
        std::array<Elem, BuffSize> ring_buff_;
    };


    template<typename Elem, std::uint32_t NumThreads>
    inline bool MPSC<Elem, NumThreads>::produce(Elem elem) {

        auto current_write = write_idx_.load(std::memory_order::memory_order_acquire);

        do {
            //test for full buffer
            if (position(current_write + 1) == position(read_idx_.load(std::memory_order::memory_order_acquire))) {
                return false;
            }

        } while (!write_idx_.compare_exchange_weak(current_write, current_write + 1, std::memory_order_acq_rel));

        ring_buff_[position(current_write)] = std::move(elem);

        auto current_write_test = current_write;
        while (!max_read_idx_.compare_exchange_weak(current_write_test, current_write + 1, std::memory_order_acq_rel)) {
            //the cas operation writes the current value into current_write_test.
            //that changes subsequent tests in a way we don't want so we restore the value on failure.
            current_write_test = current_write;
            std::this_thread::yield();
        }

        return true;
    }


    template<typename Elem, std::uint32_t NumThreads>
    inline std::optional<Elem> MPSC<Elem, NumThreads>::consume() {

        while (true) {
            auto current_read = read_idx_.load(std::memory_order::memory_order_acquire);
            auto current_max_read = max_read_idx_.load(std::memory_order::memory_order_acquire);

            //ensure there's a slot ready to read
            if (position(current_read) == position(current_max_read)) {
                return {};
            }

            auto elem = std::move(ring_buff_[position(current_read)]);

            if (read_idx_.compare_exchange_weak(current_read, current_read + 1,  std::memory_order_acq_rel)) {
                return elem;
            }
        }
    }
}


#endif //EVENTVIEW_MPSC_H
