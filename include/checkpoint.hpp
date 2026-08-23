#pragma once

#include <string>
#include <fstream>
#include <cstdint>
#include <stdexcept>
#include "state_machine.hpp"

// 二进制格式 v6:
// Header (32 bytes):
//   Magic: "OBCK" (4 bytes)
//   Version: int32 (4 bytes)
//   Price decimals: int32 (4 bytes)
//   Timestamp: int64 (8 bytes)
//   ts_divisor: int64 (8 bytes)
//   Flags: int32 (4 bytes)      bit0 = is_snapshot 覆盖语义
//                               bit1 = local_timestamp 因果时钟 + 完整消息边界
//                               bit2 = snapshot 越界幽灵档清理
//                               bit3 = snapshot 段完整缓存后原子应用
// Body:
//   Bids count: int64
//   [price(int32) + amount(double)] * N
//   Asks count: int64
//   [price(int32) + amount(double)] * M
//
// v2 (28 字节头, 无 flags) 仍可读入, flags 视为 0 —— 即"旧语义(快照当增量叠加)"。
// 两种语义的簿不可混用: 从 v2 检查点续算会把旧语义的幽灵档带进新结果。

constexpr char CHECKPOINT_MAGIC[] = "OBCK";
constexpr int32_t CHECKPOINT_VERSION = 6;
constexpr int32_t CKPT_FLAG_SNAPSHOT_AWARE = 1;
constexpr int32_t CKPT_FLAG_CAUSAL_LOCAL_TIME = 2;
constexpr int32_t CKPT_FLAG_CROSS_CLEANUP = 4;
constexpr int32_t CKPT_FLAG_ATOMIC_SNAPSHOT = 8;

inline void StateMachine::save_checkpoint(const std::string& filepath) const {
    if (in_block_) {
        throw std::runtime_error(
            "save_checkpoint: 有未生效的快照块挂起(" + std::to_string(blk_bids_.size() + blk_asks_.size())
            + " 行), 先调用 flush_snapshot()");
    }
    std::ofstream out(filepath, std::ios::binary);
    if (!out) throw std::runtime_error("cannot open file: " + filepath);

    // Header
    out.write(CHECKPOINT_MAGIC, 4);
    int32_t version = CHECKPOINT_VERSION;
    out.write(reinterpret_cast<const char*>(&version), sizeof(version));
    int32_t decimals = price_decimals_;
    out.write(reinterpret_cast<const char*>(&decimals), sizeof(decimals));
    int64_t ts = last_ts_;
    out.write(reinterpret_cast<const char*>(&ts), sizeof(ts));
    int64_t divisor = ts_divisor_;
    out.write(reinterpret_cast<const char*>(&divisor), sizeof(divisor));
    int32_t flags = snapshot_reset_enabled_
        ? (CKPT_FLAG_SNAPSHOT_AWARE | CKPT_FLAG_CAUSAL_LOCAL_TIME |
           CKPT_FLAG_CROSS_CLEANUP | CKPT_FLAG_ATOMIC_SNAPSHOT) : 0;
    out.write(reinterpret_cast<const char*>(&flags), sizeof(flags));

    // Bids
    auto bids = book_.get_all_bids();
    int64_t bid_count = bids.size();
    out.write(reinterpret_cast<const char*>(&bid_count), sizeof(bid_count));
    for (const auto& [price, amount] : bids) {
        int32_t p = price;
        out.write(reinterpret_cast<const char*>(&p), sizeof(p));
        out.write(reinterpret_cast<const char*>(&amount), sizeof(amount));
    }

    // Asks
    auto asks = book_.get_all_asks();
    int64_t ask_count = asks.size();
    out.write(reinterpret_cast<const char*>(&ask_count), sizeof(ask_count));
    for (const auto& [price, amount] : asks) {
        int32_t p = price;
        out.write(reinterpret_cast<const char*>(&p), sizeof(p));
        out.write(reinterpret_cast<const char*>(&amount), sizeof(amount));
    }
}

inline void StateMachine::load_checkpoint(const std::string& filepath) {
    std::ifstream in(filepath, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open file: " + filepath);

    // Header
    char magic[4];
    in.read(magic, 4);
    if (std::string(magic, 4) != CHECKPOINT_MAGIC)
        throw std::runtime_error("invalid checkpoint magic");

    int32_t version;
    in.read(reinterpret_cast<char*>(&version), sizeof(version));
    if (version != 2 && version != 3 && version != 4 && version != 5 &&
        version != CHECKPOINT_VERSION)
        throw std::runtime_error("unsupported checkpoint version");

    int32_t ckpt_decimals;
    in.read(reinterpret_cast<char*>(&ckpt_decimals), sizeof(ckpt_decimals));
    // 计算 rescale factor: checkpoint decimals -> 当前 decimals
    int dec_diff = price_decimals_ - ckpt_decimals;
    double price_scale = 1.0;
    if (dec_diff > 0) {
        for (int i = 0; i < dec_diff; ++i) price_scale *= 10.0;
    } else if (dec_diff < 0) {
        for (int i = 0; i < -dec_diff; ++i) price_scale /= 10.0;
    }

    int64_t ts;
    in.read(reinterpret_cast<char*>(&ts), sizeof(ts));
    last_ts_ = ts;

    int64_t divisor;
    in.read(reinterpret_cast<char*>(&divisor), sizeof(divisor));

    int32_t flags = 0;
    if (version >= 3) {
        in.read(reinterpret_cast<char*>(&flags), sizeof(flags));
    }
    loaded_ckpt_snapshot_aware_ = version >= 6
        && (flags & (CKPT_FLAG_SNAPSHOT_AWARE | CKPT_FLAG_CAUSAL_LOCAL_TIME |
                     CKPT_FLAG_CROSS_CLEANUP | CKPT_FLAG_ATOMIC_SNAPSHOT))
           == (CKPT_FLAG_SNAPSHOT_AWARE | CKPT_FLAG_CAUSAL_LOCAL_TIME |
               CKPT_FLAG_CROSS_CLEANUP | CKPT_FLAG_ATOMIC_SNAPSHOT);

    // 清空当前状态
    book_.clear();
    snapshots_.clear();
    last_snapshot_minute_ = -1;
    rows_seen_ = 0;
    message_active_ = false;
    message_visible_ = false;
    in_block_ = false;
    blk_bids_.clear();
    blk_asks_.clear();

    // Bids
    int64_t bid_count;
    in.read(reinterpret_cast<char*>(&bid_count), sizeof(bid_count));
    for (int64_t i = 0; i < bid_count; ++i) {
        int32_t price;
        double amount;
        in.read(reinterpret_cast<char*>(&price), sizeof(price));
        in.read(reinterpret_cast<char*>(&amount), sizeof(amount));
        int rescaled = static_cast<int>(static_cast<double>(price) * price_scale + 0.5);
        book_.update_bid(rescaled, amount);
    }

    // Asks
    int64_t ask_count;
    in.read(reinterpret_cast<char*>(&ask_count), sizeof(ask_count));
    for (int64_t i = 0; i < ask_count; ++i) {
        int32_t price;
        double amount;
        in.read(reinterpret_cast<char*>(&price), sizeof(price));
        in.read(reinterpret_cast<char*>(&amount), sizeof(amount));
        int rescaled = static_cast<int>(static_cast<double>(price) * price_scale + 0.5);
        book_.update_ask(rescaled, amount);
    }
}
