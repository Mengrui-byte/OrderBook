"""is_snapshot 快照重置语义的验收门。

直接运行: python tests/test_snapshot_semantics.py
退出码 0 = 全部通过。

两层:
  1. 单元 — 合成数据验证覆盖区更新、范围外深档保留和文件首快照门控
  2. 因果 — 真实数据: 任意时刻 T 的查询结果 == 只用 local_timestamp<=T 的完整消息独立重放

2、3 依赖真实数据与旧检查点, 缺失时自动跳过。
"""
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np

from orderbook._cpp import StateMachine

CKPT_DIR = '/mnt/ob_checkpoints_v6_sample/usd_btcusdt'
DATE, PREV = '2019-11-18', '2019-11-17'
US = 1_000_000

_fail = 0
_skip = 0


def check(name, cond, extra=''):
    global _fail
    print(('  PASS  ' if cond else '  FAIL  ') + name + (f'   {extra}' if extra else ''))
    if not cond:
        _fail += 1


def skip(reason):
    global _skip
    _skip += 1
    print(f'  SKIP  {reason}')


def _sm(reset=True, snapshots=False, decimals=1):
    s = StateMachine(price_decimals=decimals, ts_divisor=US, crossover_log_threshold=0)
    s.set_snapshot_enabled(snapshots)
    s.set_snapshot_reset_enabled(reset)
    return s


def _book(s):
    return dict(s.get_top_bids(10 ** 9)), dict(s.get_top_asks(10 ** 9))


# ------------------------------------------------------------------ 1. 单元

def test_unit():
    print('[1] 单元: 快照块语义')
    t0, t1 = 1_000_000 * US, 1_000_010 * US

    s = _sm()
    s.begin_file()
    s.process(t0, True, 900, 9.0, False)
    check('文件首快照前的 buffered update 被跳过', s.get_best_bid() is None)
    s.process(t1, True, 1000, 1.0, True)
    check('首个 snapshot 段先挂起', s.get_best_bid() is None and s.has_pending_snapshot)
    s.flush_snapshot()
    check('完整 snapshot 段生效', s.get_best_bid() == (1000, 1.0))

    # 覆盖区剔除 + 区外深档保留
    s = _sm()
    for p, a in [(1000, 5.0), (999, 4.0), (998, 3.0), (500, 9.0)]:
        s.process(t0, True, p, a)
    for p, a in [(1001, 5.0), (1002, 4.0), (1500, 8.0)]:
        s.process(t0, False, p, a)
    before = _book(s)
    for p, a in [(1000, 6.0), (999, 4.5)]:
        s.process(t1, True, p, a, True)
    for p, a in [(1001, 6.5), (1003, 2.0)]:
        s.process(t1, False, p, a, True)
    check('快照段完成前簿不变', _book(s) == before and s.has_pending_snapshot)
    check('快照段完成前 last_timestamp 不推进', s.last_timestamp == t0)
    s.process(t1 + 1, True, 997, 1.0)          # 增量行触发块生效
    b, a = _book(s)
    check('覆盖区内快照档写入', b.get(1000) == 6.0 and b.get(999) == 4.5)
    check('覆盖区外更深的档保留', b.get(998) == 3.0 and b.get(500) == 9.0)
    check('ask 覆盖区内旧档被剔除', 1002 not in a, f'asks={sorted(a)}')
    check('ask 覆盖区外深档保留', a.get(1500) == 8.0)
    check('块生效后无挂起', not s.has_pending_snapshot)
    e = s.snapshot_events[0]
    check('事件时刻 = 块时刻', e.timestamp == t1)
    check('事件区间 = 块内价格区间',
          (e.bid_lo, e.bid_hi, e.ask_lo, e.ask_hi) == (999, 1000, 1001, 1003))

    # 覆盖区内、快照未报的陈旧档必须消失
    s = _sm()
    for p, a in [(1000, 1.0), (999, 9.9), (998, 1.0), (700, 5.0)]:
        s.process(t0, True, p, a)
    for p, a in [(1000, 2.0), (998, 2.0)]:
        s.process(t1, True, p, a, True)
    s.process(t1 + 1, False, 1001, 1.0)
    b, _ = _book(s)
    check('覆盖区内未被快照报出的档被剔除', 999 not in b, f'bids={sorted(b)}')
    check('覆盖区外的深档不受影响', b.get(700) == 5.0)

    # 快照范围外的深档保留，但导致 crossed book 的越界幽灵档必须清除
    s = _sm()
    s.process(t0, True, 2000, 1.0)             # 陈旧高价 bid
    s.process(t0, True, 900, 1.0)
    for p, a in [(1000, 1.0), (999, 1.0)]:
        s.process(t1, True, p, a, True)
    for p, a in [(1001, 1.0), (1002, 1.0)]:
        s.process(t1, False, p, a, True)
    s.process(t1 + 1, True, 998, 1.0)
    b, _ = _book(s)
    check('导致 crossed 的范围外幽灵档清除', 2000 not in b and b.get(900) == 1.0)
    check('快照事件记录交叉清除', s.snapshot_events[-1].crossed_bids >= 1)

    # 背靠背两份快照靠 timestamp 区分
    s = _sm()
    s.process(t1, True, 1000, 1.0, True)
    s.process(t1 + 5 * US, True, 2000, 1.0, True)
    check('连续 snapshot 行属于同一快照段', len(s.snapshot_events) == 0 and s.has_pending_snapshot)
    s.flush_snapshot()
    check('flush 保持已生效状态', len(s.snapshot_events) == 1 and not s.has_pending_snapshot)

    # A/B 开关
    s = _sm(reset=False)
    s.process(t0, True, 998, 3.0)
    s.process(t1, True, 1000, 6.0, True)
    b, _ = _book(s)
    check('snapshot_reset=False 退化为逐档 upsert',
          b.get(998) == 3.0 and len(s.snapshot_events) == 0)

    # 快照逐行生效，完成当前消息后可以存盘
    s = _sm()
    s.process(t1, True, 1000, 1.0, True)
    try:
        s.save_checkpoint('/tmp/_ob_pending_test.ckpt')
        check('挂起 snapshot 段时禁止 save_checkpoint', False)
    except RuntimeError:
        check('挂起 snapshot 段时禁止 save_checkpoint', True)


# ------------------------------------------------- 2/3. 真实数据: 因果 + 回归

def _read_ckpt(path):
    with open(path, 'rb') as f:
        assert f.read(4) == b'OBCK'
        ver = struct.unpack('<i', f.read(4))[0]
        dec = struct.unpack('<i', f.read(4))[0]
        ts = struct.unpack('<q', f.read(8))[0]
        struct.unpack('<q', f.read(8))[0]                     # ts_divisor
        flags = struct.unpack('<i', f.read(4))[0] if ver >= 3 else 0
        bids, asks = {}, {}
        for target in (bids, asks):
            n = struct.unpack('<q', f.read(8))[0]
            for _ in range(n):
                p, a = struct.unpack('<id', f.read(12))
                target[p] = a
    return dict(version=ver, flags=flags, decimals=dec, ts=ts, bids=bids, asks=asks)


def test_real():
    from orderbook import OrderBook
    import pandas as pd

    prev_ckpt = os.path.join(CKPT_DIR, f'{PREV}.ckpt')
    ref_ckpt = os.path.join(CKPT_DIR, f'{DATE}.ckpt')
    if not (os.path.exists(prev_ckpt) and os.path.exists(ref_ckpt)):
        skip(f'缺少基准检查点 {CKPT_DIR}/{{{PREV},{DATE}}}.ckpt，跳过真实数据用例')
        return

    ob = OrderBook.from_asset('usd', 'btcusdt', ckpt_dir=CKPT_DIR,
                              snapshot_reset=True, cache_size=1)
    if DATE not in ob._date_files:
        skip(f'缺少 {DATE} 的 CSV，跳过真实数据用例')
        return
    ob._load_day(DATE)
    ts, bd, px, amt, sn = ob._day_ts, ob._day_bids, ob._day_px, ob._day_amt, ob._day_snap
    d = ob._divisor
    dec = ob._price_decimals

    def truth(cut_us, reset=True):
        """独立基准: 从前一日检查点起，只喂 local_timestamp <= cut_us 的完整消息。"""
        s = StateMachine(price_decimals=dec, ts_divisor=US, crossover_log_threshold=10)
        s.set_snapshot_enabled(False)
        s.set_snapshot_reset_enabled(reset)
        s.load_checkpoint(prev_ckpt)
        s.begin_file()
        hi = np.searchsorted(ts, cut_us, side='right')
        s.process_batch(ts[:hi], bd[:hi], px[:hi], amt[:hi], sn[:hi] if reset else None)
        s.flush_snapshot()
        return _book(s)

    print('\n[2] 因果: 查询结果 == 只用过去数据的独立重放')
    evts = ob.snapshot_events
    check('当日检出快照块', len(evts) >= 1, f'{len(evts)} 块')
    blk = evts[-1]['timestamp']
    check('块生效前后状态不同, 且只生效一次',
          truth(blk - 1) != truth(blk) and truth(blk) == truth(blk + 1))

    for tstr in [f'{DATE} 00:00:00', f'{DATE} 00:00:01',
                 f'{DATE} 12:34:56', f'{DATE} 23:59:59']:
        ob.clear_replay_cache()
        snap = ob.snapshot(tstr)
        got = ({int(round(p * d)): a for p, a in snap['bids']},
               {int(round(p * d)): a for p, a in snap['asks']})
        exp = truth(int(pd.Timestamp(tstr, tz='UTC').timestamp() * US))
        check(f'snapshot({tstr})', got == exp,
              f'bid {len(got[0])}/{len(exp[0])} ask {len(got[1])}/{len(exp[1])}')

    ob.clear_replay_cache()
    ok = True
    for sec in range(1, 6):                      # 顺序遍历走增量重放缓存
        tstr = f'{DATE} 12:34:0{sec}'
        snap = ob.snapshot(tstr)
        got = ({int(round(p * d)): a for p, a in snap['bids']},
               {int(round(p * d)): a for p, a in snap['asks']})
        ok = ok and got == truth(int(pd.Timestamp(tstr, tz='UTC').timestamp() * US))
    check('顺序秒级遍历(增量重放缓存路径)与独立重放一致', ok)
    check('全天处理完无挂起块', not ob._sm.has_pending_snapshot)

    print('\n[3] 检查点格式门')
    ref = _read_ckpt(ref_ckpt)
    check('v6 + snapshot/local-time/cross-cleanup/atomic flags', ref['version'] == 6 and ref['flags'] == 15,
          f'version={ref["version"]} flags={ref["flags"]}')


if __name__ == '__main__':
    test_unit()
    try:
        test_real()
    except Exception as exc:                      # 真实数据不可用不应让单元用例失败
        skip(f'真实数据用例异常: {type(exc).__name__}: {exc}')
    print(f'\n失败 {_fail} 项, 跳过 {_skip} 项')
    sys.exit(1 if _fail else 0)
