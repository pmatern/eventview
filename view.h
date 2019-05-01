
#ifndef EVENTVIEW_VIEW_H
#define EVENTVIEW_VIEW_H

#include <optional>
#include <variant>

#include "types.h"
#include "opdispatch.h"

namespace eventview {

    template<std::uint32_t NumThreads>
    class ViewReader {
    public:
        explicit ViewReader(std::shared_ptr<OpDispatch<NumThreads>> dispatch) : dispatch_{dispatch} {}

        ViewReader(const ViewReader &) = delete;

        ViewReader &operator=(const ViewReader &) = delete;

        ViewReader(ViewReader &&) = default;

        ViewReader &operator=(ViewReader &&) = default;

        ~ViewReader() = default;

        inline const std::optional<View> read_view(const ViewDescriptor &view_desc) const noexcept;

    private:
        std::shared_ptr<OpDispatch<NumThreads>> dispatch_;
    };

    template<std::uint32_t NumThreads>
    inline const std::optional<View> ViewReader<NumThreads>::read_view(const ViewDescriptor &view_desc) const noexcept {
        try {
            auto view_future = dispatch_.read_view(view_desc);
            return view_future.get();
        } catch (std::exception &e) {
            //do something
            return {};
        } catch (...) {
            //do something else?
            return {};
        }
    }

}

#endif //EVENTVIEW_VIEW_H
