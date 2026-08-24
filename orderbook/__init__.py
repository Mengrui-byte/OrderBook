import bisect
import os
import re
import struct
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import numpy as np
import pyarrow.csv as _pa_csv
import pyarrow as _pa
import pyarrow.compute as _pc
from orderbook._cpp import StateMachine as _StateMachine, CrossOverPoint, Snapshot
from orderbook.config import CONFIG

TARDIS_ROOT = CONFIG.tardis_root


def list_assets(market=None):
    """列出所有可用的 Binance 资产。

    参数:
        market: 筛选市场类型，如 'usd', 'spot', 'coin'。None 则列出全部。

    返回:
        list[dict]: [{'market': 'usd', 'symbol': 'btcusdt', 'dir': 'binance_usd_btcusdt'}, ...]
    """
    result = []
    prefix = f'binance_{market}_' if market else 'binance_'
    for name in sorted(os.listdir(TARDIS_ROOT)):
        if not name.startswith(prefix):
            continue
        parts = name.split('_', 2)  # binance, market, symbol
        if len(parts) < 3:
            continue
        data_dir = os.path.join(TARDIS_ROOT, name, 'incremental_book_L2')
        if os.path.isdir(data_dir):
            result.append({
                'market': parts[1],
                'symbol': parts[2],
                'dir': name,
            })
    return result


class OrderBook:
    """高层 Python 封装。

    用法:
        # 方式 1: 通过 market + symbol 自动定位数据目录
        ob = OrderBook.from_asset('usd', 'btcusdt')

        # 方式 2: 手动指定目录（兼容旧接口）
        ob = OrderBook(data_dir='...', ckpt_dir='...')

        ob.build_checkpoints()
        snap = ob.snapshot('2019-12-25 12:00')
    """

    @classmethod
    def from_asset(cls, market, symbol, ckpt_dir=None,
                   ckpt_root=None,
                   ts_divisor=None,
                   crossover_log_threshold=None,
                   file_pattern=r'(\d{4}-\d{2}-\d{2}).*\.csv(?:\.gz)?$',
                   cache_size=None, snapshot_reset=True):
        """通过 Binance market + symbol 创建 OrderBook。

        参数:
            market: 'usd', 'spot', 'coin'
            symbol: 'btcusdt', 'ethusdt' 等
            ckpt_dir: 检查点目录，直接指定完整路径。优先于 ckpt_root。
            ckpt_root: 检查点根目录，当 ckpt_dir 为 None 时使用，
                       实际路径为 ckpt_root/market_symbol/
        """
        if ckpt_root is None:
            ckpt_root = CONFIG.checkpoint_root
        if ts_divisor is None:
            ts_divisor = CONFIG.ts_divisor
        if crossover_log_threshold is None:
            crossover_log_threshold = CONFIG.crossover_log_threshold
        if cache_size is None:
            cache_size = CONFIG.cache_size

        asset_dir = f'binance_{market}_{symbol}'
        data_dir = os.path.join(TARDIS_ROOT, asset_dir, 'incremental_book_L2')
        if not os.path.isdir(data_dir):
            available = list_assets(market)
            symbols = [a['symbol'] for a in available[:20]]
            raise ValueError(
                f"数据目录不存在: {data_dir}\n"
                f"可用的 {market} 资产 (前20): {symbols}"
            )
        if ckpt_dir is None:
            ckpt_dir = os.path.join(ckpt_root, f'{market}_{symbol}')
        return cls(data_dir=data_dir, ckpt_dir=ckpt_dir,
                   ts_divisor=ts_divisor,
                   crossover_log_threshold=crossover_log_threshold,
                   file_pattern=file_pattern, cache_size=cache_size,
                   snapshot_reset=snapshot_reset)

    def __init__(self, data_dir, ckpt_dir, ts_divisor=None,
                 crossover_log_threshold=None,
                 file_pattern=r'(\d{4}-\d{2}-\d{2}).*\.csv(?:\.gz)?$',
                 cache_size=None, snapshot_reset=True):
        """snapshot_reset: True(默认) 按 is_snapshot 做快照重置; False 退化为旧行为
        (快照行当普通增量叠加)，仅用于 A/B 对比。"""
        if ts_divisor is None:
            ts_divisor = CONFIG.ts_divisor
        if crossover_log_threshold is None:
            crossover_log_threshold = CONFIG.crossover_log_threshold
        if cache_size is None:
            cache_size = CONFIG.cache_size
        self._data_dir = data_dir
        self._ckpt_dir = ckpt_dir
        self._ts_divisor = ts_divisor
        self._crossover_log_threshold = crossover_log_threshold
        self._cache_size = max(0, int(cache_size))  # 0 = 禁用缓存
        self._snapshot_reset = bool(snapshot_reset)

        os.makedirs(ckpt_dir, exist_ok=True)

        # 扫描数据目录: date_str -> filepath
        self._date_files = self._scan_data(file_pattern)
        # 排序后的日期列表
        self._dates = sorted(self._date_files.keys())

        # 自动检测 price_decimals
        self._price_decimals = self._detect_price_decimals()
        self._divisor = 10 ** self._price_decimals

        # L1: 分钟快照 (C++ StateMachine)
        self._sm = None
        self._loaded_date = None
        # L2: 当天原始数据 (用于秒级重放)
        self._day_ts = None
        self._day_bids = None
        self._day_px = None
        self._day_amt = None
        self._day_snap = None
        # LRU 缓存: date_str -> (sm, day_ts, day_bids, day_px, day_amt)，减少跨天重复 I/O
        self._day_cache = OrderedDict() if self._cache_size > 0 else None
        self._cache_lock = threading.Lock()  # 保护 _day_cache 与预取写入
        # 单线程预取：后台加载下一日，与主线程计算重叠
        self._prefetch_executor = ThreadPoolExecutor(max_workers=1) if self._cache_size > 0 else None
        # 秒级重放增量缓存: (minute, target_us, sm)，顺序查询时从上一秒增量重放
        self._replay_cache = None

    # ============ 一次性构建检查点 ============

    def _new_sm(self, snapshot_enabled=True):
        sm = _StateMachine(
            price_decimals=self._price_decimals, ts_divisor=self._ts_divisor,
            crossover_log_threshold=self._crossover_log_threshold
        )
        sm.set_snapshot_reset_enabled(self._snapshot_reset)
        if not snapshot_enabled:
            sm.set_snapshot_enabled(False)
        return sm

    def _read_and_prepare(self, path):
        """读取 CSV，使用 local_timestamp 作为严格因果时钟。"""
        return self._read_csv_to_numpy(path)

    def build_checkpoints(self, n_days=None, force=False, on_day_done=None, io_workers=None):
        """逐天处理数据，每天存一个检查点。I/O、计算、存盘三阶段异步重叠。

        参数:
            n_days: 处理前 N 天，None 则处理全部
            force: True 则全部重建，False 则从最后一个已有检查点继续
            on_day_done: 回调函数 fn(date_str, sm, row_count)，每天处理完后调用
            io_workers: I/O 预读线程数，默认 3（并行解压 3 个 gzip 文件）
        """
        import time as _time
        from collections import deque

        if io_workers is None:
            io_workers = CONFIG.io_workers
        sm = self._new_sm(snapshot_enabled=False)  # 构建检查点时不需要分钟快照

        total = min(n_days, len(self._dates)) if n_days else len(self._dates)

        start_idx = 0
        if not force:
            for i in range(total - 1, -1, -1):
                if os.path.exists(self._ckpt_path(self._dates[i])):
                    sm.load_checkpoint(self._ckpt_path(self._dates[i]))
                    if self._snapshot_reset and not sm.loaded_ckpt_snapshot_aware:
                        raise RuntimeError(
                            f"检查点 {self._dates[i]} 是旧语义(快照当增量叠加)生成的，"
                            f"不能与快照重置语义混用——续算会把旧语义的幽灵档带进新结果。\n"
                            f"用 build_checkpoints(force=True) 全部重建，"
                            f"或 OrderBook(..., snapshot_reset=False) 保持旧语义。"
                        )
                    start_idx = i + 1
                    print(f"从检查点恢复: {self._dates[i]}, 跳过前 {start_idx} 天")
                    break

        if start_idx >= total:
            print(f"\n全部完成，共 {total} 天检查点")
            return

        io_pool = ThreadPoolExecutor(max_workers=io_workers)  # 多线程预读
        save_pool = ThreadPoolExecutor(max_workers=1)         # 异步存盘

        # 滑动窗口：预提交 io_workers 个读取任务
        read_queue = deque()  # (day_idx, future)
        for i in range(start_idx, min(start_idx + io_workers, total)):
            future = io_pool.submit(
                self._read_and_prepare, self._date_files[self._dates[i]])
            read_queue.append((i, future))

        save_future = None  # 上一天的存盘任务
        next_submit_idx = start_idx + io_workers  # 下一个待提交的日期索引

        total_io_wait = 0.0    # 主线程等 I/O 的累计时间
        total_compute = 0.0    # process_batch 的累计时间
        total_save_wait = 0.0  # 主线程等存盘的累计时间

        for i in range(start_idx, total):
            date = self._dates[i]
            sm.clear_crossover_events()

            # 等待上一天存盘完成（save 读 sm，process_batch 写 sm，不能并发）
            if save_future is not None:
                t0 = _time.monotonic()
                save_future.result()
                total_save_wait += _time.monotonic() - t0

            # 从队列头取出当天数据（FIFO 保证顺序）
            day_idx, read_future = read_queue.popleft()
            assert day_idx == i, f"队列乱序: 期望 {i}, 实际 {day_idx}"

            # 等待当天数据就绪
            t0 = _time.monotonic()
            try:
                row_count, ts, bids, px, amt, snap, has_snap = read_future.result()
            except Exception as e:
                total_io_wait += _time.monotonic() - t0
                io_pool.shutdown(wait=False, cancel_futures=True)
                save_pool.shutdown(wait=False, cancel_futures=True)
                raise RuntimeError(
                    f"[{i+1}/{total}] {date} CSV 读取失败；为避免生成带日期缺口的检查点，已中止"
                ) from e
            total_io_wait += _time.monotonic() - t0

            # 提交下一个待读取的日期（维持滑动窗口）
            if next_submit_idx < total:
                future = io_pool.submit(
                    self._read_and_prepare, self._date_files[self._dates[next_submit_idx]])
                read_queue.append((next_submit_idx, future))
                next_submit_idx += 1

            # 计算：主线程做 btree 更新，后台多线程在并行读取后续天
            t0 = _time.monotonic()
            sm.begin_file()
            if not has_snap:
                sm.set_snapshot_context(True, False)
            sm.process_batch(ts, bids, px, amt, snap)
            # 完成文件最后一个 local_timestamp 消息。
            sm.flush_snapshot()
            total_compute += _time.monotonic() - t0
            del ts, bids, px, amt, snap

            # 异步存盘：提交后不等待，与下一轮的 read_future.result() 重叠
            ckpt_path = self._ckpt_path(date)
            save_future = save_pool.submit(sm.save_checkpoint, ckpt_path)

            print(f"[{i+1:3d}/{total}] {date}  rows={row_count:>8,}")
            if on_day_done is not None:
                on_day_done(date, sm, row_count)

        # 等待最后一天的存盘完成
        if save_future is not None:
            save_future.result()

        io_pool.shutdown(wait=False)
        save_pool.shutdown(wait=False)

        processed = total - start_idx
        print(f"\n全部完成，共 {total} 天检查点")
        print(f"  异步调度统计 ({processed} 天, {io_workers} 个 I/O 线程):")
        print(f"    主线程等 I/O:    {total_io_wait:8.1f}s  (占比 {total_io_wait/(total_io_wait+total_compute)*100:.0f}%)" if total_io_wait + total_compute > 0 else "")
        print(f"    主线程计算:      {total_compute:8.1f}s  (占比 {total_compute/(total_io_wait+total_compute)*100:.0f}%)" if total_io_wait + total_compute > 0 else "")
        print(f"    主线程等存盘:    {total_save_wait:8.1f}s")
        if total_io_wait > total_compute:
            print(f"  → I/O 瓶颈: 计算等 I/O，{total_io_wait - total_compute:.1f}s 浪费在等待")
        else:
            print(f"  → 计算瓶颈: I/O 等计算，异步预读完全隐藏了 I/O 延迟")

    # ============ 快照查询 ============

    def snapshot(self, ts):
        """查询任意时刻的快照。

        分钟级 (如 '12:00') → L1 缓存直接返回
        秒级   (如 '12:00:30') → L1 取整分钟快照 + L2 重放到目标秒

        参数:
            ts: '2019-12-25 12:00:30' / pd.Timestamp / datetime
        """
        t = pd.Timestamp(ts)
        date_str = t.strftime('%Y-%m-%d')

        if self._loaded_date != date_str:
            self._load_day(date_str)

        unix_sec = t.timestamp()
        minute = int(unix_sec) // 60
        # 整分钟也必须严格按目标时间重放，L1 快照可能在目标时间之后生成。
        return self._replay_to_second(minute, t)

    def snapshot_seconds(self, start, end, freq_sec=1):
        """查询时间范围内每秒（或指定间隔）的快照，顺序遍历时自动使用增量重放缓存。

        适用场景：订单簿合成、因子计算等需遍历秒级时刻。比逐次 snapshot() 更省事。

        参数:
            start, end: 起止时刻，同一天内
            freq_sec: 采样间隔秒数，默认 1
        返回:
            list[(timestamp, snapshot)]
        """
        t0 = pd.Timestamp(start)
        t1 = pd.Timestamp(end)
        if t0.strftime('%Y-%m-%d') != t1.strftime('%Y-%m-%d'):
            raise ValueError("start 与 end 必须在同一天内")
        result = []
        t = t0
        while t <= t1:
            snap = self.snapshot(t)
            result.append((t, snap))
            t = t + pd.Timedelta(seconds=freq_sec)
        return result

    def clear_replay_cache(self):
        """清空秒级增量重放缓存。跨分钟乱序查询时可主动调用以释放内存。"""
        self._replay_cache = None

    def close(self):
        """关闭预取线程池。对象不再使用时调用，避免后台线程常驻。"""
        if self._prefetch_executor is not None:
            self._prefetch_executor.shutdown(wait=False)
            self._prefetch_executor = None

    def snapshot_range(self, start, end):
        """查询时间范围内所有分钟快照（必须在同一天内）。"""
        t0 = pd.Timestamp(start)
        t1 = pd.Timestamp(end)
        date_str = t0.strftime('%Y-%m-%d')

        if self._loaded_date != date_str:
            self._load_day(date_str)

        m0 = int(t0.timestamp()) // 60
        m1 = int(t1.timestamp()) // 60
        minutes = self._sm.list_snapshot_minutes()
        return [
            self._format_snapshot(self._sm.get_snapshot_by_minute(m))
            for m in minutes
            if m0 <= m <= m1
        ]

    def list_snapshot_times(self):
        """列出当前已加载日期的所有快照时刻。"""
        if self._sm is None:
            return pd.DatetimeIndex([])
        minutes = self._sm.list_snapshot_minutes()
        return pd.to_datetime([m * 60 for m in minutes], unit="s", utc=True)

    # ============ 实时查询 ============

    def best_bid(self):
        r = self._sm.get_best_bid()
        if r is None:
            return None
        return (r[0] / self._divisor, r[1])

    def best_ask(self):
        r = self._sm.get_best_ask()
        if r is None:
            return None
        return (r[0] / self._divisor, r[1])

    def top_bids(self, n=10):
        return [(p / self._divisor, a) for p, a in self._sm.get_top_bids(n)]

    def top_asks(self, n=10):
        return [(p / self._divisor, a) for p, a in self._sm.get_top_asks(n)]

    def crossover(self):
        c = self._sm.get_crossover()
        if not c.has_crossover:
            return None
        return {
            "bid_price": c.bid_price / self._divisor,
            "bid_amount": c.bid_amount,
            "ask_price": c.ask_price / self._divisor,
            "ask_amount": c.ask_amount,
            "spread": c.spread / self._divisor,
        }

    # ============ 属性 ============

    @property
    def dates(self):
        return list(self._dates)

    @property
    def loaded_date(self):
        return self._loaded_date

    @property
    def snapshot_count(self):
        if self._sm is None:
            return 0
        return self._sm.snapshot_count

    @property
    def crossover_events(self):
        """当前已加载日期的 crossover 事件列表。"""
        if self._sm is None:
            return []
        return [
            {
                "timestamp": e.timestamp,
                "direction": "bid→ask" if e.bid_covers_ask else "ask→bid",
                "trigger_price": e.trigger_price / self._divisor,
                "best_bid_before": e.best_bid_before / self._divisor,
                "best_ask_before": e.best_ask_before / self._divisor,
                "cleared_count": e.cleared_count,
            }
            for e in self._sm.crossover_events
        ]

    @property
    def snapshot_events(self):
        """当前已加载日期内，每次 is_snapshot 快照生效的审计记录。

        用途：据此给"快照后深档尚未重新积累"的窗口打 warm-up 标记。
        字段：timestamp / bid_rows / ask_rows / bid_lo / bid_hi / ask_lo / ask_hi
              (覆盖区间，已还原为浮点价) / erased_* (区间内被剔除的旧档数)
              / crossed_* (交叉幽灵档清除数) / book_*_after (生效后全簿档数)
        """
        if self._sm is None:
            return []
        d = self._divisor
        out = []
        for e in self._sm.snapshot_events:
            out.append({
                "timestamp": e.timestamp,
                "bid_rows": e.bid_rows,
                "ask_rows": e.ask_rows,
                "bid_lo": e.bid_lo / d if e.bid_lo <= e.bid_hi else None,
                "bid_hi": e.bid_hi / d if e.bid_lo <= e.bid_hi else None,
                "ask_lo": e.ask_lo / d if e.ask_lo <= e.ask_hi else None,
                "ask_hi": e.ask_hi / d if e.ask_lo <= e.ask_hi else None,
                "erased_bids": e.erased_bids,
                "erased_asks": e.erased_asks,
                "crossed_bids": e.crossed_bids,
                "crossed_asks": e.crossed_asks,
                "book_bids_after": e.book_bids_after,
                "book_asks_after": e.book_asks_after,
            })
        return out

    # ============ 内部方法 ============

    @staticmethod
    def _read_ckpt_decimals(path):
        """从 checkpoint 文件头读取 price_decimals（v2-v6），失败返回 None。"""
        try:
            with open(path, 'rb') as f:
                magic = f.read(4)
                if magic != b'OBCK':
                    return None
                version = struct.unpack('<i', f.read(4))[0]
                if version not in (2, 3, 4, 5, 6):
                    return None
                return struct.unpack('<i', f.read(4))[0]
        except Exception:
            return None

    @staticmethod
    def _infer_decimals_from_prices(prices, sample_size=10000):
        """从浮点价格数组推断小数位数。取众数。"""
        sample = prices[:sample_size]
        sample = sample[sample > 0]
        if len(sample) == 0:
            return 2  # 保守默认
        # 转字符串数小数位
        from collections import Counter
        counts = Counter()
        for p in sample:
            s = f'{p:.10f}'.rstrip('0')
            dec = len(s.split('.')[1]) if '.' in s else 0
            counts[dec] += 1
        return counts.most_common(1)[0][0]

    def _detect_price_decimals(self):
        """自动检测 price_decimals。优先从 CSV 采样推断，无 CSV 则读 checkpoint 文件头。"""
        # 1. 采样第一个 CSV 的价格列
        if self._dates:
            first_path = self._date_files[self._dates[0]]
            try:
                table = _pa_csv.read_csv(first_path, convert_options=_pa_csv.ConvertOptions(
                    include_columns=['price'],
                    column_types={'price': _pa.float64()},
                ))
                prices = table.column('price').to_numpy()
                return self._infer_decimals_from_prices(prices)
            except Exception:
                pass
        # 2. 回退：读任意一个已有 checkpoint
        if os.path.isdir(self._ckpt_dir):
            for f in os.listdir(self._ckpt_dir):
                if f.endswith('.ckpt'):
                    dec = self._read_ckpt_decimals(os.path.join(self._ckpt_dir, f))
                    if dec is not None:
                        return dec
        return 2  # 最终默认

    _USECOLS = ['timestamp', 'local_timestamp', 'side', 'price', 'amount', 'is_snapshot']
    _PA_COLUMN_TYPES = {
        'timestamp': _pa.int64(), 'local_timestamp': _pa.int64(), 'side': _pa.string(),
        'price': _pa.float64(), 'amount': _pa.float64(),
        'is_snapshot': _pa.bool_(),
    }

    def _read_csv_to_numpy(self, path):
        """纯 pyarrow 管线: CSV.gz → Arrow Table → numpy 数组。

        避免 pandas 中间态和 df['side']=='bid' 的 GIL 瓶颈。
        返回 (row_count, local_ts, bids, px, amt, snap)。
        无 is_snapshot 列的数据(非 Tardis 格式)返回全 False。
        """
        try:
            table = _pa_csv.read_csv(path, convert_options=_pa_csv.ConvertOptions(
                include_columns=self._USECOLS,
                column_types=self._PA_COLUMN_TYPES,
            ))
            has_snap = True
        except _pa.ArrowInvalid as exc:
            if "is_snapshot" not in str(exc) or "does not exist" not in str(exc):
                raise
            table = _pa_csv.read_csv(path, convert_options=_pa_csv.ConvertOptions(
                include_columns=self._USECOLS[:5],
                column_types={k: v for k, v in self._PA_COLUMN_TYPES.items()
                              if k != 'is_snapshot'},
            ))
            has_snap = False
        row_count = table.num_rows
        # Tardis 以 local_timestamp 标识消息到达及消息边界。用交易所
        # timestamp 做 cutoff 会引入尚未到达的数据，不能满足绝对因果。
        ts = table.column("local_timestamp").to_numpy()
        bids = _pc.equal(table.column("side"), "bid").to_numpy(zero_copy_only=False)
        px = np.ascontiguousarray(
            (table.column("price").to_numpy() * self._divisor).astype(np.int32))
        amt = table.column("amount").to_numpy()
        if has_snap:
            snap = np.ascontiguousarray(
                table.column("is_snapshot").to_numpy(zero_copy_only=False).astype(bool))
        else:
            snap = np.zeros(row_count, dtype=bool)
        return row_count, ts, bids, px, amt, snap, has_snap

    def _scan_data(self, file_pattern):
        regex = re.compile(file_pattern)
        candidates = {}
        for f in os.listdir(self._data_dir):
            m = regex.search(f)
            if m:
                candidates.setdefault(m.group(1), []).append(f)

        result = {}
        for date_str, names in candidates.items():
            names.sort()
            if len(names) > 1:
                sizes = {os.path.getsize(os.path.join(self._data_dir, n)) for n in names}
                if len(sizes) > 1:
                    raise ValueError(
                        f"同一日期存在大小不同的数据文件 {date_str}: {names}; "
                        "请清理重复或冲突文件后再运行。"
                    )
            # 同尺寸重复文件按文件名稳定选择，避免 os.listdir 顺序影响结果。
            result[date_str] = os.path.join(self._data_dir, names[0])
        return result

    def _ckpt_path(self, date_str):
        return os.path.join(self._ckpt_dir, f'{date_str}.ckpt')

    def _prev_date(self, date_str):
        idx = bisect.bisect_left(self._dates, date_str)
        if idx == 0 or self._dates[idx] != date_str:
            return None
        return self._dates[idx - 1]

    def _most_recent_ckpt_date_before(self, date_str):
        """仅返回紧邻数据日的检查点，禁止跨缺口恢复状态。"""
        idx = bisect.bisect_left(self._dates, date_str)
        if idx > 0 and os.path.exists(self._ckpt_path(self._dates[idx - 1])):
            return self._dates[idx - 1]
        return None

    def _load_day(self, date_str):
        """加载最近一次存在的检查点 + 处理当天数据 + 缓存原始数据。"""
        if date_str not in self._date_files:
            raise ValueError(f"无数据文件: {date_str}，可用范围: {self._dates[0]} ~ {self._dates[-1]}")

        self._replay_cache = None  # 换日时作废秒级增量缓存

        # 命中缓存则直接恢复，避免 CSV + checkpoint 的重复 I/O
        if self._day_cache is not None:
            hit = False
            with self._cache_lock:
                if date_str in self._day_cache:
                    self._day_cache.move_to_end(date_str)
                    sm, day_ts, day_bids, day_px, day_amt, day_snap = self._day_cache[date_str]
                    self._sm = sm
                    self._day_ts, self._day_bids, self._day_px, self._day_amt, self._day_snap = \
                        day_ts, day_bids, day_px, day_amt, day_snap
                    self._loaded_date = date_str
                    hit = True
            if hit:
                self._schedule_prefetch(date_str)
                return

        sm = self._new_sm()  # 查询时需要分钟快照 (默认 snapshot_enabled=True)

        prev_ckpt_date = self._most_recent_ckpt_date_before(date_str)
        if prev_ckpt_date is not None:
            sm.load_checkpoint(self._ckpt_path(prev_ckpt_date))
            if self._snapshot_reset and not sm.loaded_ckpt_snapshot_aware:
                raise RuntimeError(
                    f"检查点 {prev_ckpt_date} 是旧语义检查点，不能用于绝对因果重放；"
                    "请使用新的检查点目录或先 force=True 重建。"
                )
        elif bisect.bisect_left(self._dates, date_str) > 0:
            raise RuntimeError(
                f"缺少 {date_str} 紧邻前一数据日的 v6 检查点；"
                "不能在未知深档状态下做绝对因果重放。"
            )

        _, day_ts, day_bids, day_px, day_amt, day_snap, has_snap = self._read_csv_to_numpy(
            self._date_files[date_str])

        # L1: 处理全天数据, 生成分钟快照
        sm.begin_file()
        if not has_snap:
            sm.set_snapshot_context(True, False)
        sm.process_batch(day_ts, day_bids, day_px, day_amt, day_snap)
        sm.flush_snapshot()   # 当天数据读完

        self._sm = sm
        self._day_ts, self._day_bids, self._day_px, self._day_amt, self._day_snap = \
            day_ts, day_bids, day_px, day_amt, day_snap
        self._loaded_date = date_str

        # 写入 LRU 缓存并调度预取下一日
        if self._day_cache is not None:
            with self._cache_lock:
                self._day_cache[date_str] = (sm, day_ts, day_bids, day_px, day_amt, day_snap)
                self._day_cache.move_to_end(date_str)
                while len(self._day_cache) > self._cache_size:
                    self._day_cache.popitem(last=False)
            self._schedule_prefetch(date_str)

    def _schedule_prefetch(self, date_str):
        """调度后台预取下一自然日，与主线程计算重叠。"""
        if self._prefetch_executor is None:
            return
        idx = bisect.bisect_left(self._dates, date_str)
        if idx >= len(self._dates) or self._dates[idx] != date_str:
            return
        if idx + 1 >= len(self._dates):
            return
        next_date = self._dates[idx + 1]
        with self._cache_lock:
            if next_date in self._day_cache:
                return
        self._prefetch_executor.submit(self._prefetch_day, next_date)

    def _prefetch_day(self, date_str):
        """后台线程：读取并计算一整日数据，写入缓存。与主线程解耦。"""
        if date_str not in self._date_files:
            return
        try:
            sm = self._new_sm()
            prev_ckpt_date = self._most_recent_ckpt_date_before(date_str)
            if prev_ckpt_date is not None:
                sm.load_checkpoint(self._ckpt_path(prev_ckpt_date))
                if self._snapshot_reset and not sm.loaded_ckpt_snapshot_aware:
                    raise RuntimeError(
                        f"检查点 {prev_ckpt_date} 是旧语义检查点，不能用于绝对因果重放；"
                    )
            elif bisect.bisect_left(self._dates, date_str) > 0:
                return
            _, day_ts, day_bids, day_px, day_amt, day_snap, has_snap = self._read_csv_to_numpy(
                self._date_files[date_str])
            sm.begin_file()
            if not has_snap:
                sm.set_snapshot_context(True, False)
            sm.process_batch(day_ts, day_bids, day_px, day_amt, day_snap)
            sm.flush_snapshot()
        except Exception:
            return
        with self._cache_lock:
            if self._day_cache is None:
                return
            self._day_cache[date_str] = (sm, day_ts, day_bids, day_px, day_amt, day_snap)
            self._day_cache.move_to_end(date_str)
            while len(self._day_cache) > self._cache_size:
                self._day_cache.popitem(last=False)

    def _replay_to_second(self, minute, target_ts):
        """从整分钟快照（或上一秒缓存）开始，重放到目标秒。顺序查询时利用增量缓存。

        切片边界按 timestamp 取（searchsorted side='right'），而快照块内所有行共享同一
        timestamp，因此切片永远不会把一个块切成两半：块要么整体在 [lo, hi) 内、要么整体在外。
        切片末尾的 flush_snapshot() 因此不是前视——被 flush 的块其所有行都 <= target_us。
        """
        target_us = int(target_ts.timestamp() * self._ts_divisor)

        # 尝试增量重放：缓存存在且同一分钟内、目标时间晚于缓存时间
        if self._replay_cache is not None:
            cache_min, cache_us, cache_sm, cache_row = self._replay_cache
            if cache_min == minute and cache_us < target_us:
                lo = cache_row
                hi = np.searchsorted(self._day_ts, target_us, side='right')
                if lo < hi:
                    cache_sm.process_batch(
                        self._day_ts[lo:hi],
                        self._day_bids[lo:hi],
                        self._day_px[lo:hi],
                        self._day_amt[lo:hi],
                        self._day_snap[lo:hi],
                    )
                    if cache_sm.has_pending_snapshot and hi < len(self._day_ts):
                        cache_sm.discard_snapshot()
                        self._replay_cache = None
                        return self._format_snapshot(cache_sm.get_current_snapshot())
                    cache_sm.flush_snapshot()
                self._replay_cache = (minute, target_us, cache_sm, hi)
                return self._format_snapshot(cache_sm.get_current_snapshot())

        # 回退到完整重放：只使用 local_timestamp <= target 的最近分钟快照。
        self._replay_cache = None
        snap = self._sm.get_snapshot_by_minute(minute)
        if snap.timestamp > target_us:
            snap = self._sm.get_snapshot_by_minute(minute - 1)

        tmp = self._new_sm(snapshot_enabled=False)

        tmp.load_from_snapshot(snap)

        # 起点用快照记录的行号, 不能用 searchsorted(timestamp):
        # 同一 timestamp 常有几十行(一条行情消息含多档), 分钟快照只拍到其中第一行之后,
        # 按 timestamp 跳过会把该时刻剩余的行全部丢掉。
        lo = snap.row_index
        hi = np.searchsorted(self._day_ts, target_us, side='right')
        if snap.timestamp == 0 or lo > hi:
            # 目标早于所选 L1 快照，必须从当天文件开头重新重放。
            tmp = self._new_sm(snapshot_enabled=False)
            prev_ckpt_date = self._most_recent_ckpt_date_before(self._loaded_date)
            if prev_ckpt_date is not None:
                tmp.load_checkpoint(self._ckpt_path(prev_ckpt_date))
            tmp.begin_file()
            lo = 0
        else:
            # L1 可能在一个连续 snapshot 段中间生成；回到该段首行，
            # 让覆盖区间从完整段重新建立，避免遗漏段首价格范围。
            run_start = lo
            while run_start > 0 and bool(self._day_snap[run_start - 1]):
                run_start -= 1
            if run_start < lo:
                # 不能把已包含段中间状态的 L1 再重放段首行；从前一日
                # 检查点重新开始当天，保证快照段完整且无重复应用。
                prev_ckpt_date = self._most_recent_ckpt_date_before(self._loaded_date)
                if prev_ckpt_date is not None:
                    tmp.load_checkpoint(self._ckpt_path(prev_ckpt_date))
                tmp.begin_file()
                lo = 0
            else:
                previous_is_snapshot = bool(self._day_snap[lo - 1]) if lo > 0 else False
                tmp.set_snapshot_context(True, previous_is_snapshot)
        if lo < hi:
            tmp.process_batch(
                self._day_ts[lo:hi],
                self._day_bids[lo:hi],
                self._day_px[lo:hi],
                self._day_amt[lo:hi],
                self._day_snap[lo:hi],
            )
            if tmp.has_pending_snapshot and hi < len(self._day_ts):
                tmp.discard_snapshot()
                self._replay_cache = None
                return self._format_snapshot(tmp.get_current_snapshot())
            tmp.flush_snapshot()

        self._replay_cache = (minute, target_us, tmp, max(lo, hi))
        return self._format_snapshot(tmp.get_current_snapshot())

    def snapshot_range_to_shm(self, start, end, shm_store):
        """将时间范围内的快照直接写入共享内存，跳过 list-of-tuples 中间格式。

        Args:
            start: 起始时间
            end: 结束时间
            shm_store: SharedSnapshotStore (已 allocate)

        Returns:
            写入的快照数
        """
        import numpy as _np
        t0 = pd.Timestamp(start)
        t1 = pd.Timestamp(end)
        date_str = t0.strftime('%Y-%m-%d')

        if self._loaded_date != date_str:
            self._load_day(date_str)

        m0 = int(t0.timestamp()) // 60
        m1 = int(t1.timestamp()) // 60
        minutes = self._sm.list_snapshot_minutes()
        divisor = self._divisor

        idx = 0
        for m in minutes:
            if m < m0 or m > m1:
                continue
            snap = self._sm.get_snapshot_by_minute(m)

            raw_bids = snap.bids  # list of (int_price, float_amount)
            raw_asks = snap.asks

            n_bids = len(raw_bids)
            n_asks = len(raw_asks)

            if n_bids > 0:
                bids_arr = _np.array(raw_bids, dtype=_np.float64)
                bids_arr[:, 0] /= divisor
            else:
                bids_arr = _np.empty((0, 2), dtype=_np.float64)

            if n_asks > 0:
                asks_arr = _np.array(raw_asks, dtype=_np.float64)
                asks_arr[:, 0] /= divisor
            else:
                asks_arr = _np.empty((0, 2), dtype=_np.float64)

            shm_store.write_snapshot(idx, snap.timestamp, bids_arr, asks_arr)
            idx += 1

        return idx

    def scan_day_levels(self, start, end):
        """预扫描一天的快照，返回每个快照的档位数（用于分配共享内存）。

        Returns:
            (timestamps, bid_counts, ask_counts) 三个 list
        """
        t0 = pd.Timestamp(start)
        t1 = pd.Timestamp(end)
        date_str = t0.strftime('%Y-%m-%d')

        if self._loaded_date != date_str:
            self._load_day(date_str)

        m0 = int(t0.timestamp()) // 60
        m1 = int(t1.timestamp()) // 60
        minutes = self._sm.list_snapshot_minutes()

        timestamps = []
        bid_counts = []
        ask_counts = []
        for m in minutes:
            if m < m0 or m > m1:
                continue
            snap = self._sm.get_snapshot_by_minute(m)
            timestamps.append(snap.timestamp)
            bid_counts.append(len(snap.bids))
            ask_counts.append(len(snap.asks))

        return timestamps, bid_counts, ask_counts

    def load_day_to_shm(self, date_str):
        """一次遍历加载一天快照到共享内存（合并 scan + allocate + write）。

        Args:
            date_str: 日期字符串 YYYY-MM-DD

        Returns:
            (SharedSnapshotStore, n_snaps) — 调用方负责 close()
            如果没有快照，返回 (None, 0)
        """
        import numpy as _np
        from shared_snapshot_store import SharedSnapshotStore

        start = f"{date_str} 00:00:00"
        end = f"{date_str} 23:59:59"
        t0 = pd.Timestamp(start)
        t1 = pd.Timestamp(end)

        if self._loaded_date != date_str:
            self._load_day(date_str)

        m0 = int(t0.timestamp()) // 60
        m1 = int(t1.timestamp()) // 60
        minutes = self._sm.list_snapshot_minutes()
        divisor = self._divisor

        # 一次遍历：收集所有快照数据到内存
        snap_data = []  # [(timestamp, bids_arr, asks_arr), ...]
        total_bids = 0
        total_asks = 0

        for m in minutes:
            if m < m0 or m > m1:
                continue
            snap = self._sm.get_snapshot_by_minute(m)
            raw_bids = snap.bids
            raw_asks = snap.asks

            if raw_bids:
                bids_arr = _np.array(raw_bids, dtype=_np.float64)
                bids_arr[:, 0] /= divisor
            else:
                bids_arr = _np.empty((0, 2), dtype=_np.float64)

            if raw_asks:
                asks_arr = _np.array(raw_asks, dtype=_np.float64)
                asks_arr[:, 0] /= divisor
            else:
                asks_arr = _np.empty((0, 2), dtype=_np.float64)

            snap_data.append((snap.timestamp, bids_arr, asks_arr))
            total_bids += len(bids_arr)
            total_asks += len(asks_arr)

        n_snaps = len(snap_data)
        if n_snaps == 0:
            return None, 0

        # 分配共享内存并写入
        store = SharedSnapshotStore.allocate(
            n_snapshots=n_snaps,
            total_bid_levels=total_bids,
            total_ask_levels=total_asks,
            price_divisor=1.0,
        )

        for idx, (timestamp, bids_arr, asks_arr) in enumerate(snap_data):
            store.write_snapshot(idx, timestamp, bids_arr, asks_arr)

        return store, n_snaps

    def _format_snapshot(self, snap):
        return {
            "timestamp": snap.timestamp,
            "minute": snap.minute,
            "bids": [(p / self._divisor, a) for p, a in snap.bids],
            "asks": [(p / self._divisor, a) for p, a in snap.asks],
        }
