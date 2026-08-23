#!/usr/bin/env python3
"""
BTreeOrderBook API 用法演示
============================

展示 orderbook 库的全部 Python API 调用方式，包括：
  1. list_assets()       — 列出可用资产
  2. OrderBook.from_asset() / OrderBook() — 创建实例
  3. build_checkpoints() — 构建检查点
  4. snapshot()          — 分钟级 / 秒级快照查询
  5. snapshot_range()    — 范围查询
  6. snapshot_seconds()  — 秒级遍历
  7. list_snapshot_times() — 列出快照时刻
  8. best_bid/ask, top_bids/asks — 盘口查询
  9. crossover / crossover_events — Crossover 检测
 10. 属性: dates, loaded_date, snapshot_count
 11. clear_replay_cache / close — 资源管理
 12. StateMachine 底层 API  — 直接操作 C++ 引擎
"""

import numpy as np
import pandas as pd
from orderbook import CONFIG, OrderBook, list_assets
from orderbook._cpp import StateMachine, CrossOverPoint, Snapshot


def sep(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ============================================================
# 1. list_assets — 列出可用资产
# ============================================================
sep("1. list_assets() — 列出可用资产")

# 列出全部 Binance 资产
all_assets = list_assets()
print(f"全部资产数: {len(all_assets)}")

# 按市场筛选
usd_assets = list_assets('usd')      # USD-M 合约
spot_assets = list_assets('spot')    # 现货
coin_assets = list_assets('coin')    # 币本位合约
print(f"USD-M: {len(usd_assets)}, 现货: {len(spot_assets)}, 币本位: {len(coin_assets)}")

# 每条记录的结构
print(f"示例: {usd_assets[0]}")
# {'market': 'usd', 'symbol': 'btcusdt', 'dir': 'binance_usd_btcusdt'}


# ============================================================
# 2. 创建 OrderBook 实例
# ============================================================
sep("2. 创建 OrderBook 实例")

# 方式 1: from_asset (推荐)
ob = OrderBook.from_asset(
    market='usd',
    symbol='ethusdt',
    ts_divisor=1_000_000,           # Tardis 时间戳为微秒
    crossover_log_threshold=10,     # spread > 10 个最小价格单位才记录 crossover
)
print(f"from_asset 创建成功, 共 {len(ob.dates)} 天数据")

# 方式 2: from_asset 指定检查点目录
ob2 = OrderBook.from_asset(
    market='usd',
    symbol='ethusdt',
    ckpt_dir=f'{CONFIG.checkpoint_root}/usd_ethusdt',      # 指定完整路径
)
print(f"指定 ckpt_dir 创建成功")
del ob2

# 方式 3: 手动指定目录 (兼容旧接口)
# ob3 = OrderBook(
#     data_dir='/path/to/TardisSource/binance_usd_ethusdt/incremental_book_L2',
#     ckpt_dir='/path/to/checkpoints/usd_ethusdt',
#     ts_divisor=1_000_000,
# )

# from_asset 其他可选参数:
# ckpt_root     — 检查点根目录, 默认来自 orderbook/config.ini
# file_pattern  — CSV 文件名正则, 默认匹配 YYYY-MM-DD*.csv(.gz)
# cache_size    — LRU 天缓存大小, 默认 5 (0 = 禁用)


# ============================================================
# 3. 属性
# ============================================================
sep("3. 属性")

print(f"ob.dates          — 可用日期列表, 共 {len(ob.dates)} 天")
print(f"  首日: {ob.dates[0]}")
print(f"  末日: {ob.dates[-1]}")
print(f"ob.loaded_date    — 当前已加载日期: {ob.loaded_date}")
print(f"ob.snapshot_count — 当前快照数: {ob.snapshot_count}")


# ============================================================
# 4. build_checkpoints — 构建检查点
# ============================================================
sep("4. build_checkpoints() — 构建检查点")

# 已有检查点时会自动跳过
# ob.build_checkpoints()                   # 处理全部
# ob.build_checkpoints(n_days=30)          # 只处理前 30 天
# ob.build_checkpoints(force=True)         # 强制全部重建
# ob.build_checkpoints(io_workers=5)       # 5 个 I/O 预读线程

# 带回调
# ob.build_checkpoints(
#     on_day_done=lambda date, sm, rows: print(f"  {date}: {rows} rows")
# )

print("(已有 2309 天检查点, 跳过构建)")


# ============================================================
# 5. snapshot — 分钟级 / 秒级快照查询
# ============================================================
sep("5. snapshot() — 分钟级 / 秒级快照查询")

# 分钟级查询 — 命中 L1 缓存, O(1)
snap = ob.snapshot('2024-06-15 12:00')
print(f"分钟级快照 2024-06-15 12:00:")
print(f"  timestamp: {snap['timestamp']}")
print(f"  minute:    {snap['minute']}")
print(f"  bids 档数: {len(snap['bids'])}")
print(f"  asks 档数: {len(snap['asks'])}")
print(f"  Top 3 bids: {snap['bids'][:3]}")
print(f"  Top 3 asks: {snap['asks'][:3]}")

# 秒级查询 — L1 整分钟快照 + L2 重放
snap_sec = ob.snapshot('2024-06-15 12:00:30')
print(f"\n秒级快照 2024-06-15 12:00:30:")
print(f"  bids 档数: {len(snap_sec['bids'])}")
print(f"  asks 档数: {len(snap_sec['asks'])}")
print(f"  Top 3 bids: {snap_sec['bids'][:3]}")
print(f"  Top 3 asks: {snap_sec['asks'][:3]}")

# 也接受 pd.Timestamp / datetime
snap_ts = ob.snapshot(pd.Timestamp('2024-06-15 12:01'))
print(f"\npd.Timestamp 输入: bids={len(snap_ts['bids'])}, asks={len(snap_ts['asks'])}")


# ============================================================
# 6. snapshot_range — 范围查询 (分钟级)
# ============================================================
sep("6. snapshot_range() — 范围查询")

snaps = ob.snapshot_range('2024-06-15 12:00', '2024-06-15 12:10')
print(f"2024-06-15 12:00 ~ 12:10 共 {len(snaps)} 个分钟快照")
for s in snaps[:3]:
    bb = s['bids'][0] if s['bids'] else None
    ba = s['asks'][0] if s['asks'] else None
    print(f"  minute={s['minute']}, best_bid={bb}, best_ask={ba}")
print(f"  ...")


# ============================================================
# 7. snapshot_seconds — 秒级遍历
# ============================================================
sep("7. snapshot_seconds() — 秒级遍历")

# 遍历 12:00:00 ~ 12:00:05, 每秒一个快照
sec_snaps = ob.snapshot_seconds('2024-06-15 12:00:00', '2024-06-15 12:00:05', freq_sec=1)
print(f"12:00:00 ~ 12:00:05 共 {len(sec_snaps)} 个秒级快照")
for ts, s in sec_snaps:
    bb = s['bids'][0] if s['bids'] else None
    print(f"  {ts} -> best_bid={bb}")


# ============================================================
# 8. list_snapshot_times — 列出快照时刻
# ============================================================
sep("8. list_snapshot_times()")

times = ob.list_snapshot_times()
print(f"当天快照时刻数: {len(times)}")
print(f"类型: {type(times)}")     # pd.DatetimeIndex
print(f"前 5 个: {times[:5].tolist()}")
print(f"后 5 个: {times[-5:].tolist()}")


# ============================================================
# 9. 盘口查询
# ============================================================
sep("9. 盘口查询")

bid = ob.best_bid()
ask = ob.best_ask()
print(f"best_bid(): {bid}")        # (price, amount) or None
print(f"best_ask(): {ask}")        # (price, amount) or None
if bid and ask:
    print(f"spread: {ask[0] - bid[0]:.2f}")

top5_bids = ob.top_bids(5)
top5_asks = ob.top_asks(5)
print(f"\ntop_bids(5):")
for price, amount in top5_bids:
    print(f"  {price:.2f} x {amount:.4f}")
print(f"top_asks(5):")
for price, amount in top5_asks:
    print(f"  {price:.2f} x {amount:.4f}")


# ============================================================
# 10. crossover — Crossover 检测
# ============================================================
sep("10. crossover() / crossover_events")

# 当前 crossover 状态
cross = ob.crossover()
print(f"crossover(): {cross}")
# None 表示无 crossover; 否则为 dict:
# {"bid_price", "bid_amount", "ask_price", "ask_amount", "spread"}

# 当天 crossover 事件列表
events = ob.crossover_events
print(f"crossover_events: {len(events)} 个事件")
if events:
    for e in events[:3]:
        print(f"  ts={e['timestamp']}, {e['direction']}, "
              f"trigger={e['trigger_price']:.2f}, cleared={e['cleared_count']}")
    if len(events) > 3:
        print(f"  ... 共 {len(events)} 个")


# ============================================================
# 11. clear_replay_cache / close — 资源管理
# ============================================================
sep("11. 资源管理")

# 清空秒级增量重放缓存 (跨分钟乱序查询时释放内存)
ob.clear_replay_cache()
print("clear_replay_cache() 完成")

# close() 关闭预取线程池 (对象不再使用时调用)
# ob.close()
print("close() — 关闭预取线程池 (此处跳过, 后续还要用)")


# ============================================================
# 12. 跨天查询
# ============================================================
sep("12. 跨天查询 — 自动加载 + LRU 缓存")

# 查询不同日期会自动加载对应天, LRU 缓存减少重复 I/O
snap_a = ob.snapshot('2024-06-14 23:59')
print(f"2024-06-14 23:59 -> loaded_date={ob.loaded_date}, "
      f"bids={len(snap_a['bids'])}")

snap_b = ob.snapshot('2024-06-15 00:01')
print(f"2024-06-15 00:01 -> loaded_date={ob.loaded_date}, "
      f"bids={len(snap_b['bids'])}")

# 切回前一天 — 命中 LRU 缓存, 无需重新读 CSV
snap_c = ob.snapshot('2024-06-14 12:00')
print(f"2024-06-14 12:00 -> loaded_date={ob.loaded_date}, "
      f"bids={len(snap_c['bids'])} (LRU 缓存命中)")


# ============================================================
# 13. StateMachine 底层 C++ API
# ============================================================
sep("13. StateMachine 底层 C++ API")

sm = StateMachine(
    price_decimals=2,
    ts_divisor=1_000_000,
    crossover_log_threshold=0,
)

# --- 单条处理 ---
sm.process(timestamp=1705276800_000000, is_bid=True, price=300000, amount=1.5)
sm.process(timestamp=1705276800_000000, is_bid=False, price=300100, amount=2.0)
sm.process(timestamp=1705276800_100000, is_bid=True, price=300050, amount=0.8)

print("单条 process():")
print(f"  best_bid: {sm.get_best_bid()}")     # (300050, 0.8)
print(f"  best_ask: {sm.get_best_ask()}")     # (300100, 2.0)

# --- 批量处理 (numpy) ---
ts = np.array([1705276860_000000, 1705276860_000001, 1705276860_000002], dtype=np.int64)
bids = np.array([True, False, True], dtype=bool)
px = np.array([300200, 300250, 300180], dtype=np.int32)
amt = np.array([1.0, 0.5, 3.0], dtype=np.float64)

sm.process_batch(ts, bids, px, amt)
print(f"\n批量 process_batch() 后:")
print(f"  best_bid: {sm.get_best_bid()}")
print(f"  best_ask: {sm.get_best_ask()}")

# --- 查询 ---
print(f"\n  get_top_bids(3): {sm.get_top_bids(3)}")
print(f"  get_top_asks(3): {sm.get_top_asks(3)}")

cross = sm.get_crossover()
print(f"\n  get_crossover():")
print(f"    has_crossover: {cross.has_crossover}")
print(f"    bid_price: {cross.bid_price}, ask_price: {cross.ask_price}")
print(f"    spread: {cross.spread}")

# --- 快照 ---
print(f"\n  snapshot_count: {sm.snapshot_count}")
minutes = sm.list_snapshot_minutes()
print(f"  list_snapshot_minutes(): {minutes}")

if minutes:
    snap = sm.get_snapshot_by_minute(minutes[0])
    print(f"  get_snapshot_by_minute({minutes[0]}):")
    print(f"    timestamp: {snap.timestamp}")
    print(f"    minute:    {snap.minute}")
    print(f"    bids: {snap.bids[:3]}")
    print(f"    asks: {snap.asks[:3]}")

cur = sm.get_current_snapshot()
print(f"\n  get_current_snapshot():")
print(f"    timestamp: {cur.timestamp}, bids={len(cur.bids)}, asks={len(cur.asks)}")

# --- 快照开关 ---
sm.set_snapshot_enabled(False)
print(f"\n  set_snapshot_enabled(False) — 关闭快照 (构建检查点时用)")
sm.set_snapshot_enabled(True)

# --- 属性 ---
print(f"\n  last_timestamp:  {sm.last_timestamp}")
print(f"  price_decimals:  {sm.price_decimals}")
print(f"  ts_divisor:      {sm.ts_divisor}")
print(f"  snapshot_count:  {sm.snapshot_count}")

# --- crossover_events ---
events = sm.crossover_events
print(f"\n  crossover_events: {len(events)} 个")
for e in events[:3]:
    print(f"    ts={e.timestamp}, bid_covers_ask={e.bid_covers_ask}, "
          f"trigger={e.trigger_price}, cleared={e.cleared_count}")

sm.clear_crossover_events()
print(f"  clear_crossover_events() 后: {len(sm.crossover_events)} 个")

# --- 检查点 ---
import tempfile, os
ckpt_file = os.path.join(tempfile.gettempdir(), 'demo_sm.ckpt')
sm.save_checkpoint(ckpt_file)
print(f"\n  save_checkpoint('{ckpt_file}') — {os.path.getsize(ckpt_file)} bytes")

sm2 = StateMachine(price_decimals=2, ts_divisor=1_000_000)
sm2.load_checkpoint(ckpt_file)
print(f"  load_checkpoint() -> best_bid={sm2.get_best_bid()}, best_ask={sm2.get_best_ask()}")
os.remove(ckpt_file)

# --- load_from_snapshot ---
snap = sm.get_current_snapshot()
sm3 = StateMachine(price_decimals=2, ts_divisor=1_000_000)
sm3.load_from_snapshot(snap)
print(f"\n  load_from_snapshot() -> best_bid={sm3.get_best_bid()}")


# ============================================================
# 14. scan_day_levels / snapshot_range_to_shm (高级)
# ============================================================
sep("14. scan_day_levels() — 预扫描档位数")

# 先确保加载了某一天
_ = ob.snapshot('2024-06-15 12:00')

timestamps, bid_counts, ask_counts = ob.scan_day_levels(
    '2024-06-15 00:00', '2024-06-15 23:59')
print(f"2024-06-15 共 {len(timestamps)} 个分钟快照")
if timestamps:
    print(f"  首个: ts={timestamps[0]}, bids={bid_counts[0]}, asks={ask_counts[0]}")
    print(f"  末个: ts={timestamps[-1]}, bids={bid_counts[-1]}, asks={ask_counts[-1]}")
    print(f"  最大 bids 档数: {max(bid_counts)}")
    print(f"  最大 asks 档数: {max(ask_counts)}")


# ============================================================
# Done
# ============================================================
sep("Done")
ob.close()
print("全部 API 演示完成!")
