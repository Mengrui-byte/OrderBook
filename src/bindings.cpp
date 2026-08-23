#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include "checkpoint.hpp"

namespace py = pybind11;

PYBIND11_MODULE(_cpp, m) {
    m.doc() = "High-performance order book based on absl::btree_map";

    py::class_<CrossOverPoint>(m, "CrossOverPoint")
        .def_readonly("has_crossover", &CrossOverPoint::has_crossover)
        .def_readonly("bid_price", &CrossOverPoint::bid_price)
        .def_readonly("bid_amount", &CrossOverPoint::bid_amount)
        .def_readonly("ask_price", &CrossOverPoint::ask_price)
        .def_readonly("ask_amount", &CrossOverPoint::ask_amount)
        .def_readonly("spread", &CrossOverPoint::spread);

    py::class_<Snapshot>(m, "Snapshot")
        .def_readonly("timestamp", &Snapshot::timestamp)
        .def_readonly("minute", &Snapshot::minute)
        .def_readonly("row_index", &Snapshot::row_index)
        .def_readonly("bids", &Snapshot::bids)
        .def_readonly("asks", &Snapshot::asks);

    py::class_<SnapshotEvent>(m, "SnapshotEvent")
        .def_readonly("timestamp", &SnapshotEvent::timestamp)
        .def_readonly("bid_rows", &SnapshotEvent::bid_rows)
        .def_readonly("ask_rows", &SnapshotEvent::ask_rows)
        .def_readonly("bid_lo", &SnapshotEvent::bid_lo)
        .def_readonly("bid_hi", &SnapshotEvent::bid_hi)
        .def_readonly("ask_lo", &SnapshotEvent::ask_lo)
        .def_readonly("ask_hi", &SnapshotEvent::ask_hi)
        .def_readonly("erased_bids", &SnapshotEvent::erased_bids)
        .def_readonly("erased_asks", &SnapshotEvent::erased_asks)
        .def_readonly("crossed_bids", &SnapshotEvent::crossed_bids)
        .def_readonly("crossed_asks", &SnapshotEvent::crossed_asks)
        .def_readonly("book_bids_after", &SnapshotEvent::book_bids_after)
        .def_readonly("book_asks_after", &SnapshotEvent::book_asks_after);

    py::class_<CrossOverEvent>(m, "CrossOverEvent")
        .def_readonly("timestamp", &CrossOverEvent::timestamp)
        .def_readonly("bid_covers_ask", &CrossOverEvent::bid_covers_ask)
        .def_readonly("trigger_price", &CrossOverEvent::trigger_price)
        .def_readonly("best_bid_before", &CrossOverEvent::best_bid_before)
        .def_readonly("best_ask_before", &CrossOverEvent::best_ask_before)
        .def_readonly("cleared_count", &CrossOverEvent::cleared_count);

    py::class_<StateMachine>(m, "StateMachine")
        .def(py::init<int, int64_t, int>(),
             py::arg("price_decimals") = 1,
             py::arg("ts_divisor") = 1000000,  // 默认微秒
             py::arg("crossover_log_threshold") = 0)

        // 单条处理
        .def("process", &StateMachine::process,
             py::arg("timestamp"), py::arg("is_bid"),
             py::arg("price"), py::arg("amount"),
             py::arg("is_snapshot") = false)

        // 批量处理 (numpy) — 释放 GIL 以允许预取线程与主线程并发
        // is_snapshots 省略/None 时全部按增量处理(旧调用兼容)
        .def("process_batch", [](StateMachine& sm,
                                  py::array_t<int64_t> timestamps,
                                  py::array_t<bool> is_bids,
                                  py::array_t<int> prices,
                                  py::array_t<double> amounts,
                                  std::optional<py::array_t<bool>> is_snapshots) {
            if (timestamps.size() != is_bids.size()
                || timestamps.size() != prices.size()
                || timestamps.size() != amounts.size()) {
                throw std::invalid_argument("array lengths must match");
            }
            if (is_snapshots && is_snapshots->size() != timestamps.size()) {
                throw std::invalid_argument("is_snapshots length must match");
            }
            const auto* ts_ptr = timestamps.data();
            const auto* bid_ptr = is_bids.data();
            const auto* px_ptr = prices.data();
            const auto* amt_ptr = amounts.data();
            const bool* snap_ptr = is_snapshots ? is_snapshots->data() : nullptr;
            const auto count = static_cast<size_t>(timestamps.size());
            {
                py::gil_scoped_release release;
                sm.process_batch(ts_ptr, bid_ptr, px_ptr, amt_ptr, count, snap_ptr);
            }
        }, py::arg("timestamps"), py::arg("is_bids"),
           py::arg("prices"), py::arg("amounts"),
           py::arg("is_snapshots") = py::none())

        // 快照块
        .def("flush_snapshot", &StateMachine::flush_snapshot)
        .def("discard_snapshot", &StateMachine::discard_snapshot)
        .def("begin_file", &StateMachine::begin_file)
        .def("set_snapshot_context", &StateMachine::set_snapshot_context,
             py::arg("initialized"), py::arg("previous_is_snapshot"))
        .def_property_readonly("has_pending_snapshot", &StateMachine::has_pending_snapshot)
        .def_property_readonly("pending_snapshot_rows", &StateMachine::pending_snapshot_rows)
        .def("set_snapshot_reset_enabled", &StateMachine::set_snapshot_reset_enabled,
             py::arg("enabled"))
        .def_property_readonly("snapshot_reset_enabled", &StateMachine::snapshot_reset_enabled)
        .def_property_readonly("snapshot_events", &StateMachine::snapshot_events)
        .def("clear_snapshot_events", &StateMachine::clear_snapshot_events)
        .def_property_readonly("loaded_ckpt_snapshot_aware",
                               &StateMachine::loaded_ckpt_snapshot_aware)

        // 查询
        .def("get_best_bid", &StateMachine::get_best_bid)
        .def("get_best_ask", &StateMachine::get_best_ask)
        .def("get_top_bids", &StateMachine::get_top_bids, py::arg("n") = 10)
        .def("get_top_asks", &StateMachine::get_top_asks, py::arg("n") = 10)
        .def("get_crossover", &StateMachine::get_crossover)

        // 快照
        .def("get_snapshot", &StateMachine::get_snapshot, py::arg("timestamp"))
        .def("get_snapshot_by_minute", &StateMachine::get_snapshot_by_minute, py::arg("minute"))
        .def("get_current_snapshot", &StateMachine::get_current_snapshot)
        .def("list_snapshot_minutes", &StateMachine::list_snapshot_minutes)

        // 检查点 — 纯文件 I/O，释放 GIL
        .def("save_checkpoint", &StateMachine::save_checkpoint, py::arg("filepath"),
             py::call_guard<py::gil_scoped_release>())
        .def("load_checkpoint", &StateMachine::load_checkpoint, py::arg("filepath"),
             py::call_guard<py::gil_scoped_release>())

        // 状态
        .def_property_readonly("last_timestamp", &StateMachine::last_timestamp)
        .def_property_readonly("price_decimals", &StateMachine::price_decimals)
        .def_property_readonly("ts_divisor", &StateMachine::ts_divisor)
        .def_property_readonly("snapshot_count", &StateMachine::snapshot_count)
        .def_property_readonly("crossover_events", &StateMachine::crossover_events)
        .def("clear_crossover_events", &StateMachine::clear_crossover_events)
        .def("set_snapshot_enabled", &StateMachine::set_snapshot_enabled,
             py::arg("enabled"))
        .def("load_from_snapshot", &StateMachine::load_from_snapshot,
             py::arg("snapshot"));
}
