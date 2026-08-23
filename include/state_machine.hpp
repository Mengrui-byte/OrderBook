#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include "absl/container/btree_map.h"
#include "orderbook.hpp"

struct Snapshot {
    int64_t timestamp;  // 原始时间戳
    int64_t minute;     // 分钟编号 (unix minutes since epoch)
    int64_t row_index;  // 拍照时已处理的行数 = 重放该快照之后应从第几行继续
                        // (同一 timestamp 常有多行, 只靠 timestamp 定位会丢行)
    std::vector<std::pair<int, double>> bids;
    std::vector<std::pair<int, double>> asks;
};

struct CrossOverEvent {
    int64_t timestamp;       // 发生时刻
    bool bid_covers_ask;     // true: 新 bid 覆盖 ask, false: 新 ask 覆盖 bid
    int trigger_price;       // 触发 crossover 的价格
    int best_bid_before;     // 修正前 best bid
    int best_ask_before;     // 修正前 best ask
    size_t cleared_count;    // 被清除的档数
};

// 一次快照(is_snapshot=true 块)生效的审计记录。
// 用途: 下游据此给"快照后深档尚未积累"的窗口打 warm-up 标记。
struct SnapshotEvent {
    int64_t timestamp;        // 快照块的时刻(块内所有行同一 timestamp)
    size_t bid_rows;          // 块内 bid 行数(含 amount<=0 的删档行)
    size_t ask_rows;
    int bid_lo, bid_hi;       // 块的 bid 覆盖区间(整数价); 无 bid 行时 lo > hi
    int ask_lo, ask_hi;
    size_t erased_bids;       // 覆盖区内被剔除的旧档数
    size_t erased_asks;
    size_t crossed_bids;      // 与快照盘口交叉、被清掉的残留旧档数
    size_t crossed_asks;
    size_t book_bids_after;   // 生效后全簿档数
    size_t book_asks_after;
};

class StateMachine {
public:
    explicit StateMachine(int price_decimals = 1, int64_t ts_divisor = 1000000,
                          int crossover_log_threshold = 0);

    // ============ 数据处理 ============

    // is_snapshot=false: 普通增量, 逐档 upsert(amount<=0 删档)。
    // is_snapshot=true : Tardis 重连快照行。false -> true 开始新快照段；
    //                   逐行覆盖该段已观测到的价格范围，范围外深档保留。
    void process(int64_t timestamp, bool is_bid, int price, double amount,
                 bool is_snapshot = false);

    // is_snapshots 可为 nullptr(全部按增量处理), 保持旧调用兼容。
    void process_batch(const int64_t* timestamps,
                       const bool* is_bids,
                       const int* prices,
                       const double* amounts,
                       size_t count,
                       const bool* is_snapshots = nullptr);

    // ============ 快照块 ============

    // 完成当前 local_timestamp 消息并生成必要的分钟缓存。
    void flush_snapshot() { finish_message(); apply_snapshot_block(); }
    void discard_snapshot() {
        in_block_ = false;
        blk_bids_.clear();
        blk_asks_.clear();
        previous_is_snapshot_ = false;
    }
    bool has_pending_snapshot() const { return in_block_; }
    size_t pending_snapshot_rows() const {
        return blk_bids_.size() + blk_asks_.size();
    }

    // 开始处理一个新的 Tardis CSV 文件。文件首个快照前的缓冲增量必须跳过。
    void begin_file();

    // 从分钟快照继续重放时恢复 is_snapshot 上下文。
    void set_snapshot_context(bool initialized, bool previous_is_snapshot);

    // 关闭后 is_snapshot 行退化为普通增量(旧行为), 用于 A/B 对比。
    void set_snapshot_reset_enabled(bool enabled) { snapshot_reset_enabled_ = enabled; }
    bool snapshot_reset_enabled() const { return snapshot_reset_enabled_; }

    const std::vector<SnapshotEvent>& snapshot_events() const { return snapshot_events_; }
    void clear_snapshot_events() { snapshot_events_.clear(); }

    // ============ 查询接口 ============

    std::optional<std::pair<int, double>> get_best_bid() const;
    std::optional<std::pair<int, double>> get_best_ask() const;
    std::vector<std::pair<int, double>> get_top_bids(int n = 10) const;
    std::vector<std::pair<int, double>> get_top_asks(int n = 10) const;
    CrossOverPoint get_crossover() const;

    // 按原始时间戳查询 (<= timestamp 的最近快照)
    Snapshot get_snapshot(int64_t timestamp) const;
    // 按分钟编号查询 (精确匹配)
    Snapshot get_snapshot_by_minute(int64_t minute) const;
    // 当前快照
    Snapshot get_current_snapshot() const;
    // 列出所有快照的分钟编号
    std::vector<int64_t> list_snapshot_minutes() const;

    // ============ 检查点 ============

    void save_checkpoint(const std::string& filepath) const;
    void load_checkpoint(const std::string& filepath);

    // ============ 状态 ============

    int64_t last_timestamp() const { return last_ts_; }
    int price_decimals() const { return price_decimals_; }
    int64_t ts_divisor() const { return ts_divisor_; }
    size_t snapshot_count() const { return snapshots_.size(); }
    const std::vector<CrossOverEvent>& crossover_events() const { return crossover_events_; }
    void clear_crossover_events() { crossover_events_.clear(); }

    // 快照开关: 构建检查点时可关闭以节省内存和 CPU
    void set_snapshot_enabled(bool enabled) { snapshot_enabled_ = enabled; }

    // 从快照恢复 order book 状态 (避免 Python 端逐条 process 调用)
    void load_from_snapshot(const Snapshot& snap);

    // 生成本状态机的检查点是否带快照重置语义(见 checkpoint.hpp flags)
    bool loaded_ckpt_snapshot_aware() const { return loaded_ckpt_snapshot_aware_; }

private:
    int64_t ts_to_minute(int64_t timestamp) const;
    void maybe_take_snapshot(int64_t timestamp);
    void finish_message();
    void take_snapshot(int64_t timestamp, int64_t minute);
    void resolve_crossover(int64_t timestamp, bool is_bid, int price);
    void apply_snapshot_block();

    OrderBook book_;
    int price_decimals_;
    int64_t ts_divisor_;          // timestamp / ts_divisor_ = unix seconds
    int crossover_log_threshold_; // 只在 spread > threshold 时记录事件
    int64_t last_ts_ = 0;
    int64_t last_snapshot_minute_ = -1;
    int64_t rows_seen_ = 0;        // 已处理行数, 用于分钟快照的 row_index
    bool snapshot_enabled_ = true; // 快照开关

    // 快照块缓冲: 块 = 同一 timestamp 的连续 is_snapshot=true 行
    bool snapshot_reset_enabled_ = true;
    bool in_block_ = false;  // 保留 ABI/API 语义；新实现不挂起快照块
    int64_t block_ts_ = 0;
    std::vector<std::pair<int, double>> blk_bids_;
    std::vector<std::pair<int, double>> blk_asks_;
    std::vector<SnapshotEvent> snapshot_events_;
    bool loaded_ckpt_snapshot_aware_ = true;
    bool message_active_ = false;
    int64_t message_ts_ = 0;
    bool message_visible_ = false;

    bool file_initialized_ = true;
    bool previous_is_snapshot_ = false;

    // key = 分钟编号 (unix timestamp in seconds / 60)
    absl::btree_map<int64_t, Snapshot> snapshots_;
    std::vector<CrossOverEvent> crossover_events_;
};

// ============ 实现 ============

inline StateMachine::StateMachine(int price_decimals, int64_t ts_divisor,
                                   int crossover_log_threshold)
    : price_decimals_(price_decimals), ts_divisor_(ts_divisor),
      crossover_log_threshold_(crossover_log_threshold) {}

inline int64_t StateMachine::ts_to_minute(int64_t timestamp) const {
    return timestamp / ts_divisor_ / 60;
}

inline void StateMachine::process(int64_t timestamp, bool is_bid,
                                   int price, double amount, bool is_snapshot) {
    if (message_active_ && timestamp != message_ts_) finish_message();
    if (!message_active_) {
        message_active_ = true;
        message_ts_ = timestamp;
        message_visible_ = false;
    }
    ++rows_seen_;

    if (snapshot_reset_enabled_) {
        // Tardis: 文件首个 snapshot 前的 buffered updates 必须跳过。
        if (!file_initialized_ && !is_snapshot) return;

        if (is_snapshot && !previous_is_snapshot_) {
            in_block_ = true;
            block_ts_ = timestamp;
            blk_bids_.clear();
            blk_asks_.clear();
            file_initialized_ = true;
        }

        if (is_snapshot) {
            (is_bid ? blk_bids_ : blk_asks_).emplace_back(price, amount);
            previous_is_snapshot_ = true;
            message_visible_ = false;
            return;
        }
        if (in_block_) apply_snapshot_block();
        previous_is_snapshot_ = false;
    }

    if (is_bid) {
        book_.update_bid(price, amount);
    } else {
        book_.update_ask(price, amount);
    }
    // 新 Tardis 语义忠实保留原始状态，包括偶发 crossed book；不能通过
    // 删除另一侧价位来“修正”数据。旧 A/B 模式保留历史行为。
    if (amount > 0 && !snapshot_reset_enabled_) {
        resolve_crossover(timestamp, is_bid, price);
    }
    last_ts_ = timestamp;
    message_visible_ = true;
}

inline void StateMachine::finish_message() {
    if (!message_active_) return;
    if (message_visible_) maybe_take_snapshot(message_ts_);
    message_active_ = false;
    message_visible_ = false;
}

inline void StateMachine::begin_file() {
    finish_message();
    if (in_block_) apply_snapshot_block();
    if (!snapshot_reset_enabled_) return;
    file_initialized_ = false;
    previous_is_snapshot_ = false;
}

inline void StateMachine::set_snapshot_context(bool initialized, bool previous_is_snapshot) {
    file_initialized_ = initialized;
    previous_is_snapshot_ = previous_is_snapshot;
}

inline void StateMachine::process_batch(const int64_t* timestamps,
                                         const bool* is_bids,
                                         const int* prices,
                                         const double* amounts,
                                         size_t count,
                                         const bool* is_snapshots) {
    if (is_snapshots == nullptr) {
        for (size_t i = 0; i < count; ++i) {
            process(timestamps[i], is_bids[i], prices[i], amounts[i], false);
        }
    } else {
        for (size_t i = 0; i < count; ++i) {
            process(timestamps[i], is_bids[i], prices[i], amounts[i], is_snapshots[i]);
        }
    }
}

inline void StateMachine::apply_snapshot_block() {
    if (!in_block_) return;

    SnapshotEvent evt{};
    evt.timestamp = block_ts_;
    evt.bid_rows = blk_bids_.size();
    evt.ask_rows = blk_asks_.size();
    evt.bid_lo = 1; evt.bid_hi = 0;   // 空区间标记 (lo > hi)
    evt.ask_lo = 1; evt.ask_hi = 0;

    int blk_best_bid = 0; bool has_blk_bid = false;
    int blk_best_ask = 0; bool has_blk_ask = false;

    if (!blk_bids_.empty()) {
        int lo = blk_bids_[0].first, hi = blk_bids_[0].first;
        for (const auto& [p, a] : blk_bids_) {
            if (p < lo) lo = p;
            if (p > hi) hi = p;
            if (a > 0 && (!has_blk_bid || p > blk_best_bid)) { blk_best_bid = p; has_blk_bid = true; }
        }
        evt.bid_lo = lo; evt.bid_hi = hi;
        evt.erased_bids = book_.erase_bids_in_range(lo, hi);
        for (const auto& [p, a] : blk_bids_) {
            if (a > 0) book_.update_bid(p, a);
        }
    }

    if (!blk_asks_.empty()) {
        int lo = blk_asks_[0].first, hi = blk_asks_[0].first;
        for (const auto& [p, a] : blk_asks_) {
            if (p < lo) lo = p;
            if (p > hi) hi = p;
            if (a > 0 && (!has_blk_ask || p < blk_best_ask)) { blk_best_ask = p; has_blk_ask = true; }
        }
        evt.ask_lo = lo; evt.ask_hi = hi;
        evt.erased_asks = book_.erase_asks_in_range(lo, hi);
        for (const auto& [p, a] : blk_asks_) {
            if (a > 0) book_.update_ask(p, a);
        }
    }

    // 快照覆盖区外导致 crossed book 的旧档是幽灵档；不影响盘口排序的
    // 更深档继续保留。若快照自身已经 crossed，则保留原始快照事实。
    if (has_blk_bid && has_blk_ask && blk_best_bid < blk_best_ask) {
        evt.crossed_bids = book_.clear_bids_ge(blk_best_ask);
        evt.crossed_asks = book_.clear_asks_le(blk_best_bid);
    } else if (has_blk_ask && !has_blk_bid) {
        evt.crossed_bids = book_.clear_bids_ge(blk_best_ask);
    } else if (has_blk_bid && !has_blk_ask) {
        evt.crossed_asks = book_.clear_asks_le(blk_best_bid);
    }

    evt.book_bids_after = book_.bid_count();
    evt.book_asks_after = book_.ask_count();
    snapshot_events_.push_back(evt);

    in_block_ = false;
    blk_bids_.clear();
    blk_asks_.clear();
    if (block_ts_ > last_ts_) last_ts_ = block_ts_;
    maybe_take_snapshot(block_ts_);
}

inline void StateMachine::maybe_take_snapshot(int64_t timestamp) {
    if (!snapshot_enabled_) return;
    int64_t minute = ts_to_minute(timestamp);
    if (minute != last_snapshot_minute_) {
        take_snapshot(timestamp, minute);
        last_snapshot_minute_ = minute;
    }
}

inline void StateMachine::take_snapshot(int64_t timestamp, int64_t minute) {
    Snapshot snap;
    snap.timestamp = timestamp;
    snap.minute = minute;
    snap.row_index = rows_seen_;
    snap.bids = book_.get_all_bids();
    snap.asks = book_.get_all_asks();
    snapshots_[minute] = std::move(snap);
}

inline Snapshot StateMachine::get_snapshot(int64_t timestamp) const {
    int64_t minute = ts_to_minute(timestamp);
    if (snapshots_.empty()) return Snapshot{};
    auto it = snapshots_.upper_bound(minute);
    if (it == snapshots_.begin()) return Snapshot{};
    --it;
    return it->second;
}

inline Snapshot StateMachine::get_snapshot_by_minute(int64_t minute) const {
    auto it = snapshots_.find(minute);
    if (it == snapshots_.end()) return Snapshot{};
    return it->second;
}

inline std::vector<int64_t> StateMachine::list_snapshot_minutes() const {
    std::vector<int64_t> result;
    result.reserve(snapshots_.size());
    for (const auto& [minute, _] : snapshots_) {
        result.push_back(minute);
    }
    return result;
}

inline Snapshot StateMachine::get_current_snapshot() const {
    Snapshot snap;
    snap.timestamp = last_ts_;
    snap.minute = ts_to_minute(last_ts_);
    snap.row_index = rows_seen_;
    snap.bids = book_.get_all_bids();
    snap.asks = book_.get_all_asks();
    return snap;
}

inline std::optional<std::pair<int, double>> StateMachine::get_best_bid() const {
    return book_.get_best_bid();
}

inline std::optional<std::pair<int, double>> StateMachine::get_best_ask() const {
    return book_.get_best_ask();
}

inline std::vector<std::pair<int, double>> StateMachine::get_top_bids(int n) const {
    return book_.get_best_n_bids(n);
}

inline std::vector<std::pair<int, double>> StateMachine::get_top_asks(int n) const {
    return book_.get_best_n_asks(n);
}

inline CrossOverPoint StateMachine::get_crossover() const {
    return book_.get_crossover();
}

inline void StateMachine::resolve_crossover(int64_t timestamp, bool is_bid, int price) {
    if (is_bid) {
        auto ba = book_.get_best_ask();
        if (ba && price >= ba->first) {
            int best_ask_before = ba->first;
            int spread = price - best_ask_before;
            size_t cleared = book_.clear_asks_le(price);
            if (spread > crossover_log_threshold_) {
                CrossOverEvent evt;
                evt.timestamp = timestamp;
                evt.bid_covers_ask = true;
                evt.trigger_price = price;
                evt.best_bid_before = price;
                evt.best_ask_before = best_ask_before;
                evt.cleared_count = cleared;
                crossover_events_.push_back(evt);
            }
        }
    } else {
        auto bb = book_.get_best_bid();
        if (bb && price <= bb->first) {
            int best_bid_before = bb->first;
            int spread = best_bid_before - price;
            size_t cleared = book_.clear_bids_ge(price);
            if (spread > crossover_log_threshold_) {
                CrossOverEvent evt;
                evt.timestamp = timestamp;
                evt.bid_covers_ask = false;
                evt.trigger_price = price;
                evt.best_bid_before = best_bid_before;
                evt.best_ask_before = price;
                evt.cleared_count = cleared;
                crossover_events_.push_back(evt);
            }
        }
    }
}

inline void StateMachine::load_from_snapshot(const Snapshot& snap) {
    book_.clear();
    in_block_ = false;          // 兼容字段复位
    blk_bids_.clear();
    blk_asks_.clear();
    for (const auto& [price, amount] : snap.bids) {
        book_.update_bid(price, amount);
    }
    for (const auto& [price, amount] : snap.asks) {
        book_.update_ask(price, amount);
    }
    last_ts_ = snap.timestamp;
    last_snapshot_minute_ = snap.minute;
    rows_seen_ = snap.row_index;
    message_active_ = false;
    message_visible_ = false;
}
