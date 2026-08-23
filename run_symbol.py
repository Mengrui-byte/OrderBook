#!/usr/bin/env python3
"""构建检查点。支持单 symbol 或多 symbol 并行。

用法:
    # 单 symbol
    python run_symbol.py ethusdt
    python run_symbol.py solusdt --force
    python run_symbol.py btcusdt --force --ckpt-root /mnt/ob_checkpoints_v4

    # 多 symbol 并行 (利用 multiprocessing)
    python run_symbol.py --all
    python run_symbol.py --all --force --workers 20

    # 手动多 symbol 后台 (旧方式仍可用)
    python run_symbol.py ethusdt &
    python run_symbol.py solusdt &
    wait
"""
import sys
import os
import time
from multiprocessing import Pool

# 确保从脚本所在目录导入 orderbook
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from orderbook import OrderBook, list_assets

DEFAULT_CKPT_ROOT = '/mnt/ob_checkpoints_v4'


def build_one(args):
    """构建单个 symbol 的检查点 (适用于 Pool.map)。"""
    symbol, force, ckpt_root = args
    ckpt_dir = os.path.join(ckpt_root, f'usd_{symbol}')

    print(f"[{symbol}] 开始构建检查点, ckpt_dir={ckpt_dir}")
    t0 = time.time()
    try:
        ob = OrderBook.from_asset(
            market='usd',
            symbol=symbol,
            ckpt_dir=ckpt_dir,
            ts_divisor=1_000_000,
            crossover_log_threshold=10,
        )
        ob.build_checkpoints(force=force)
        elapsed = time.time() - t0
        print(f"[{symbol}] 完成, 耗时 {elapsed:.1f}s")
        return symbol, True, elapsed
    except Exception as e:
        elapsed = time.time() - t0
        print(f"[{symbol}] 失败: {e}, 耗时 {elapsed:.1f}s")
        return symbol, False, elapsed


def main():
    force = '--force' in sys.argv
    workers = 20  # 默认并行数
    ckpt_root = DEFAULT_CKPT_ROOT

    # 解析 --workers N
    for i, arg in enumerate(sys.argv):
        if arg == '--workers' and i + 1 < len(sys.argv):
            workers = int(sys.argv[i + 1])
        elif arg == '--ckpt-root' and i + 1 < len(sys.argv):
            ckpt_root = sys.argv[i + 1]

    if '--all' in sys.argv:
        # 多 symbol 并行模式
        assets = list_assets('usd')
        symbols = [a['symbol'] for a in assets]
        if not symbols:
            print("未发现可用的 usd 资产")
            sys.exit(1)

        print(f"发现 {len(symbols)} 个 symbol，使用 {workers} 进程并行构建")
        t0 = time.time()

        with Pool(processes=min(workers, len(symbols))) as pool:
            results = pool.map(build_one, [(s, force, ckpt_root) for s in symbols])

        ok = sum(1 for _, success, _ in results if success)
        total_time = time.time() - t0
        print(f"\n全部完成: {ok}/{len(symbols)} 成功, 总耗时 {total_time:.1f}s")

    else:
        # 单 symbol 模式 (向后兼容)
        option_values = {
            sys.argv[i + 1] for i, a in enumerate(sys.argv[:-1])
            if a in ('--workers', '--ckpt-root')
        }
        non_flag_args = [
            a for a in sys.argv[1:]
            if not a.startswith('--') and a not in option_values
        ]
        if not non_flag_args:
            print("用法: python run_symbol.py <symbol> [--force]")
            print("      python run_symbol.py --all [--force] [--workers N]")
            sys.exit(1)

        symbol = non_flag_args[0].lower()
        build_one((symbol, force, ckpt_root))


if __name__ == '__main__':
    main()
