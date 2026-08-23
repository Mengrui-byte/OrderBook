#pragma once

#include <optional>
#include <vector>
#include <utility>
#include "absl/container/btree_map.h"

struct CrossOverPoint {
    bool has_crossover;
    int bid_price;
    double bid_amount;
    int ask_price;
    double ask_amount;
    double spread;  // ask_price - bid_price，负数表示 crossover
};

class OrderBook {
public:
    // 更新操作：amount > 0 更新，amount <= 0 删除
    void update_bid(int price, double amount);
    void update_ask(int price, double amount);
    void clear();

    // 查询最优价格 - O(1)
    std::optional<std::pair<int, double>> get_best_bid() const;
    std::optional<std::pair<int, double>> get_best_ask() const;

    // Top N - O(n)
    std::vector<std::pair<int, double>> get_best_n_bids(int n) const;
    std::vector<std::pair<int, double>> get_best_n_asks(int n) const;

    // 全量导出
    std::vector<std::pair<int, double>> get_all_bids() const;
    std::vector<std::pair<int, double>> get_all_asks() const;

    // crossover 检测 - O(1)
    CrossOverPoint get_crossover() const;

    // crossover 修正: 清除越界价位, 返回被清除的档数
    size_t clear_bids_ge(int price);   // 清除 bid >= price
    size_t clear_asks_le(int price);   // 清除 ask <= price

    // 区间清除 [lo, hi] 含端点, 返回被清除的档数 (快照生效时剔除覆盖区内的旧档)
    size_t erase_bids_in_range(int lo, int hi);
    size_t erase_asks_in_range(int lo, int hi);

    size_t bid_count() const { return bids_.size(); }
    size_t ask_count() const { return asks_.size(); }

private:
    absl::btree_map<int, double, std::greater<int>> bids_;  // 降序
    absl::btree_map<int, double>                    asks_;  // 升序
};

// ============ 实现 ============

inline void OrderBook::update_bid(int price, double amount) {
    if (amount > 0) {
        bids_[price] = amount;
    } else {
        bids_.erase(price);
    }
}

inline void OrderBook::update_ask(int price, double amount) {
    if (amount > 0) {
        asks_[price] = amount;
    } else {
        asks_.erase(price);
    }
}

inline void OrderBook::clear() {
    bids_.clear();
    asks_.clear();
}

inline std::optional<std::pair<int, double>> OrderBook::get_best_bid() const {
    if (bids_.empty()) return std::nullopt;
    auto it = bids_.begin();
    return std::make_pair(it->first, it->second);
}

inline std::optional<std::pair<int, double>> OrderBook::get_best_ask() const {
    if (asks_.empty()) return std::nullopt;
    auto it = asks_.begin();
    return std::make_pair(it->first, it->second);
}

inline std::vector<std::pair<int, double>> OrderBook::get_best_n_bids(int n) const {
    std::vector<std::pair<int, double>> result;
    result.reserve(n);
    for (auto it = bids_.begin(); it != bids_.end() && n > 0; ++it, --n) {
        result.emplace_back(it->first, it->second);
    }
    return result;
}

inline std::vector<std::pair<int, double>> OrderBook::get_best_n_asks(int n) const {
    std::vector<std::pair<int, double>> result;
    result.reserve(n);
    for (auto it = asks_.begin(); it != asks_.end() && n > 0; ++it, --n) {
        result.emplace_back(it->first, it->second);
    }
    return result;
}

inline std::vector<std::pair<int, double>> OrderBook::get_all_bids() const {
    std::vector<std::pair<int, double>> result;
    result.reserve(bids_.size());
    for (const auto& [price, amount] : bids_) {
        result.emplace_back(price, amount);
    }
    return result;
}

inline std::vector<std::pair<int, double>> OrderBook::get_all_asks() const {
    std::vector<std::pair<int, double>> result;
    result.reserve(asks_.size());
    for (const auto& [price, amount] : asks_) {
        result.emplace_back(price, amount);
    }
    return result;
}

inline CrossOverPoint OrderBook::get_crossover() const {
    CrossOverPoint cp{};
    auto bb = get_best_bid();
    auto ba = get_best_ask();
    if (!bb || !ba) {
        cp.has_crossover = false;
        return cp;
    }
    cp.bid_price  = bb->first;
    cp.bid_amount = bb->second;
    cp.ask_price  = ba->first;
    cp.ask_amount = ba->second;
    cp.spread     = ba->first - bb->first;
    cp.has_crossover = (cp.spread < 0);  // 严格: bid 必须 < ask
    return cp;
}

// bids_ 用 std::greater<int>, begin() 是最大值
// 清除所有 bid price >= threshold
inline size_t OrderBook::clear_bids_ge(int price) {
    size_t count = 0;
    while (!bids_.empty() && bids_.begin()->first >= price) {
        bids_.erase(bids_.begin());
        ++count;
    }
    return count;
}

// asks_ 用默认 less<int>, begin() 是最小值
// 清除所有 ask price <= threshold
inline size_t OrderBook::clear_asks_le(int price) {
    size_t count = 0;
    while (!asks_.empty() && asks_.begin()->first <= price) {
        asks_.erase(asks_.begin());
        ++count;
    }
    return count;
}

// bids_ 降序: lower_bound(hi) 是第一个 <= hi 的档, 往后价格递减
inline size_t OrderBook::erase_bids_in_range(int lo, int hi) {
    if (lo > hi) return 0;
    size_t count = 0;
    auto it = bids_.lower_bound(hi);
    while (it != bids_.end() && it->first >= lo) {
        it = bids_.erase(it);
        ++count;
    }
    return count;
}

// asks_ 升序: lower_bound(lo) 是第一个 >= lo 的档, 往后价格递增
inline size_t OrderBook::erase_asks_in_range(int lo, int hi) {
    if (lo > hi) return 0;
    size_t count = 0;
    auto it = asks_.lower_bound(lo);
    while (it != asks_.end() && it->first <= hi) {
        it = asks_.erase(it);
        ++count;
    }
    return count;
}
