# BTreeOrderBook

基于 `absl::btree_map` 的高性能订单簿，通过 pybind11 提供 Python 接口。适用于处理 Tardis.dev 等来源的 Binance 历史逐笔行情数据，支持多资产选择、分钟级快照缓存、秒级精度重放和二进制检查点持久化。

## 项目结构

```
BTreeOrderBook_1/
├── include/
│   ├── orderbook.hpp       # 核心订单簿 (btree_map 实现)
│   ├── state_machine.hpp   # 状态机：快照管理 + crossover 修正
│   └── checkpoint.hpp      # 二进制检查点读写
├── src/
│   └── bindings.cpp        # pybind11 绑定层
├── orderbook/
│   └── __init__.py         # Python 高层封装 (资产选择 + 双级缓存)
├── demo.ipynb              # 完整使用演示 (ETHUSDT)
├── setup.py                # Python 包构建配置
├── pyproject.toml          # PEP 517 构建声明
└── CMakeLists.txt          # CMake 构建配置 (备用)
```

## 依赖

### 系统依赖

- C++17 编译器 (GCC 7+ / Clang 5+)
- Python 3.8+

### C++ 库

- [abseil-cpp](https://github.com/abseil/abseil-cpp) — 提供 `absl::btree_map`
- [pybind11](https://github.com/pybind/pybind11) >= 2.10

### Python 库

- `pandas`
- `numpy`

### 安装依赖 (Ubuntu/Debian)

```bash
# abseil
sudo apt install libabsl-dev

# pybind11
pip install pybind11

# Python 依赖
pip install pandas numpy
```

## 构建与安装

```bash
cd BTreeOrderBook_1
pip install -e .
```

### 配置文件

运行路径和默认参数集中在 `orderbook/config.ini`：

```ini
[paths]
tardis_root = /path/to/TardisSource
checkpoint_root = /path/to/checkpoints

[defaults]
market = usd
ts_divisor = 1000000
crossover_log_threshold = 10
cache_size = 5
io_workers = 3
workers = 20
```

Python API 和 `run_symbol.py` 都读取这份配置。也可以通过环境变量选择另一份配置：

```bash
export ORDERBOOK_CONFIG=/path/to/orderbook.ini
```

显式传入 `ckpt_dir`、`ts_divisor` 等参数时，以显式参数为准。

编译默认启用 `-O3 -march=native -flto` 优化。

## 快速开始

```python
from orderbook import OrderBook, list_assets

# 查看可用的 Binance USD-M 合约资产
list_assets('usd')
# [{'market': 'usd', 'symbol': 'btcusdt', ...}, ...]

# 创建 OrderBook (以 ETHUSDT 为例)
ob = OrderBook.from_asset(
    market='usd',
    symbol='ethusdt',
    ts_divisor=1_000_000,   # Tardis 时间戳为微秒
)

# 构建检查点 (首次需要, 之后增量构建)
ob.build_checkpoints()

# 分钟级查询 — O(1) 直接返回
snap = ob.snapshot('2019-12-01 12:00')

# 秒级查询 — 从整分钟快照重放
snap = ob.snapshot('2019-12-01 12:00:30')
```

## 数据目录

数据根目录默认为 `<configured TardisSource path>`，目录结构为:

```
TardisSource/
├── binance_usd_btcusdt/incremental_book_L2/    # USD-M 合约
├── binance_usd_ethusdt/incremental_book_L2/
├── binance_spot_btcusdt/incremental_book_L2/   # 现货
├── binance_coin_btcusd_perp/incremental_book_L2/ # 币本位合约
└── ...
```

## 数据格式要求

CSV 文件需包含以下列：

| 列名          | 类型    | 说明                                                     |
|---------------|---------|----------------------------------------------------------|
| `timestamp`   | int64   | 时间戳，默认微秒精度 (Tardis 格式)                        |
| `local_timestamp` | int64 | 本地消息到达时间；因果查询和消息分组使用此列              |
| `side`        | string  | `"bid"` 或 `"ask"`                                       |
| `price`       | float   | 价格，按检测到的 `price_decimals` 转成整数存储            |
| `amount`      | float   | 数量，`<= 0` 表示删除该价位                              |
| `is_snapshot` | bool    | 可选。`true` 表示该行属于全量快照，见「快照重置语义」     |

缺少 `is_snapshot` 列的数据也能读，全部按增量处理；Tardis 数据必须保留该列。

文件命名需包含日期，如 `2024-01-15_BTCUSDT.csv.gz`，日期通过正则提取。

## 使用方法

### 资产选择

```python
from orderbook import OrderBook, list_assets

# 列出可用资产
list_assets()        # 所有 Binance 资产
list_assets('usd')   # USD-M 合约
list_assets('spot')  # 现货
list_assets('coin')  # 币本位合约

# 方式 1: 通过 market + symbol 创建 (推荐)
ob = OrderBook.from_asset('usd', 'ethusdt')

# 指定检查点目录
ob = OrderBook.from_asset('usd', 'ethusdt',
                           ckpt_dir='/path/to/my/checkpoints')

# 方式 2: 手动指定目录 (兼容旧接口)
ob = OrderBook(
    data_dir='/path/to/csv/dir',
    ckpt_dir='/path/to/checkpoints',
    ts_divisor=1_000_000,
)
```

### 构建检查点

首次使用需逐天处理数据并生成检查点文件：

```python
# 处理全部数据
ob.build_checkpoints()

# 只处理前 30 天
ob.build_checkpoints(n_days=30)

# 强制全部重建 (忽略已有检查点)
ob.build_checkpoints(force=True)

# 带回调
ob.build_checkpoints(
    on_day_done=lambda date, sm, rows: print(f"{date}: {rows} rows")
)
```

增量构建：默认从最后一个已有检查点继续，无需重新处理全部数据。

### 查询快照

```python
# 分钟级查询 — 直接命中 L1 缓存, O(1)
snap = ob.snapshot("2024-01-15 12:00")

# 秒级查询 — L1 取整分钟快照 + L2 重放到目标秒
snap = ob.snapshot("2024-01-15 12:00:30")

# 返回格式
# {
#     "timestamp": 1705312800000000,
#     "minute": 28421880,
#     "bids": [(152.50, 1.2), (152.49, 0.8), ...],
#     "asks": [(152.51, 0.5), (152.52, 2.1), ...],
# }
```

### 范围查询

```python
# 获取某时间段内所有分钟快照
snaps = ob.snapshot_range("2024-01-15 12:00", "2024-01-15 13:00")

# 列出当天所有快照时刻
times = ob.list_snapshot_times()  # 返回 pd.DatetimeIndex
```

### 盘口查询

```python
bid = ob.best_bid()       # (price, amount) 或 None
ask = ob.best_ask()       # (price, amount) 或 None
top10 = ob.top_bids(10)   # [(price, amount), ...]
cross = ob.crossover()    # None 或 {"bid_price", "ask_price", "spread", ...}
```

### Crossover 事件

```python
events = ob.crossover_events
# [{"timestamp": ..., "direction": "bid→ask", "trigger_price": 152.50, ...}, ...]
```

### 属性

```python
ob.dates           # 可用日期列表
ob.loaded_date     # 当前已加载的日期
ob.snapshot_count  # 当前已加载日期的快照数量
```

## 底层 C++ API

```python
from orderbook._cpp import StateMachine

sm = StateMachine(ts_divisor=1_000_000)

# 单条处理
sm.process(timestamp, is_bid=True, price=152.50, amount=1.2)

# 批量处理 (numpy 数组)
sm.process_batch(timestamps, is_bids, prices, amounts)

# 查询
sm.get_best_bid()           # (price, amount) 或 None
sm.get_best_ask()
sm.get_top_bids(n=10)
sm.get_crossover()          # CrossOverPoint 对象

# 快照
sm.get_snapshot(timestamp)
sm.get_snapshot_by_minute(minute)
sm.get_current_snapshot()
sm.list_snapshot_minutes()

# 快照开关 (构建检查点时关闭以提升性能)
sm.set_snapshot_enabled(False)

# 检查点
sm.save_checkpoint("state.ckpt")
sm.load_checkpoint("state.ckpt")
```

## 快照重置语义 (is_snapshot)

Tardis 的 `incremental_book_L2` 在每日文件开头和重连后提供 `is_snapshot=true` 行。
本项目面向需要保留 Binance 有限深度快照之外历史深档的研究场景，因此不整簿清空：
快照对逐侧实际覆盖到的价格区间具有权威性，区间外深档继续保留。

### 本库的处理规则

**段的定义**：前一行为 `is_snapshot=false`、当前行为 `true` 时开始新段，与 Tardis
定义的重连边界一致。文件首个快照段之前的 buffered updates 会被跳过。

**生效方式**（逐侧独立处理）：

1. 求出块内该侧的价格区间 `[lo, hi]`；
2. **剔除**簿上落在 `[lo, hi]` 内的所有旧档——快照对这个区间是权威的，它没报出来的档位即已不存在；
3. 写入块内 `amount > 0` 的档位；
4. 清除覆盖区外导致 `bid >= ask` 的越界幽灵档；不影响盘口排序的深档继续保留。

区间之外、更深的旧档**保留**。快照只覆盖交易所侧 top-1000 档（BTCUSDT 上约 ±50bps），
更深的部分快照给不出，全清会丢掉真实存在的深档；这是"不留幽灵档"与"不误删深档"之间的取舍。
`snapshot_events` 会记录每段实际覆盖区间、删除数量和生效后的档位数，供下游审计。

### 因果性

- 使用 `local_timestamp` 而非交易所 `timestamp` 作为可见性时钟。
- 同一 `local_timestamp` 的所有价位属于同一消息，全部处理完后才生成可查询缓存状态。
- 任意时刻 `T` 的查询结果等于从前日 v6 检查点开始，只喂
  `local_timestamp <= T` 的完整消息所得结果；真实数据验收会逐档校验。

### 开关与审计

```python
ob = OrderBook.from_asset('usd', 'btcusdt')                    # 默认启用
ob = OrderBook.from_asset('usd', 'btcusdt', snapshot_reset=False)  # 退化为旧行为(A/B 对比)

ob.snapshot_events   # 当日每次快照生效的审计记录
# [{'timestamp': ..., 'bid_rows': 2417, 'bid_lo': 5000.0, 'bid_hi': 33049.9,
#   'erased_bids': 32149, 'crossed_bids': 0, 'book_bids_after': 1518, ...}, ...]
```

### ⚠️ 与旧检查点不兼容

v6 检查点同时标记快照覆盖语义、`local_timestamp` 因果时钟、越界幽灵档清理和
snapshot 段原子应用语义。v2/v3/v4/v5 会被拒绝，
不能参与查询或续算。检查点目录由 `orderbook/config.ini` 的
`[paths].checkpoint_root` 控制。

`<configured checkpoint path> 下的旧检查点采用**旧语义**（快照当增量叠加），
簿里带有大量幽灵档——BTCUSDT 2022-05-31 收盘实测 32834/30688 档，启用重置后只有 19967/15450 档，
即旧口径约 **40~50% 的档位是陈旧残留**。两种语义不可混用，`build_checkpoints` 从旧检查点
续算时会直接报错。要用新语义必须 `build_checkpoints(force=True)` 全量重建。

## 架构设计

### 双级缓存

- **L1 (分钟级)**: C++ StateMachine 在每个新分钟自动生成全量快照，存储在 `btree_map` 中，O(1) 查找
- **L2 (秒级)**: 从 L1 整分钟快照出发，用缓存的 numpy 数组重放到目标秒

### 快照开关

`set_snapshot_enabled(False)` 可关闭快照生成。构建检查点时自动关闭，避免无用的全量盘口拷贝，大幅降低内存占用和 CPU 开销。

### 价格表示

CSV 的浮点价格按自动检测的 `price_decimals` 乘 `10^decimals` 转成 `int` 作为 btree_map 的 key，
避免浮点 key 的相等性问题。对外的查询接口会再除回浮点。

### L2 秒级重放的起点

分钟快照记录了拍照时已处理的行数 `row_index`，秒级重放从该行号继续。
查询 cutoff 用 `local_timestamp`；分钟缓存记录 `row_index`，重放从精确行号继续。

### Crossover 修正

新快照语义不修改 crossed book，只提供检测结果。旧 A/B 模式仍保留历史 crossover 修正行为。

### 检查点格式

二进制格式 v6，32 字节头 + 变长体；v2/v3/v4/v5 只允许底层读取，高层因果查询会拒绝：

```
Header: "OBCK" | version(i32) | price_decimals(i32) | timestamp(i64) | ts_divisor(i64) | flags(i32)
        flags bit0 = is_snapshot 覆盖语义
        flags bit1 = local_timestamp 因果时钟 + 完整消息边界
        flags bit2 = snapshot 越界幽灵档清理
        flags bit3 = snapshot 段完整缓存后原子应用
Body:   bid_count(i64) | [price(i32) + amount(f64)] * N
        ask_count(i64) | [price(i32) + amount(f64)] * M
```

## API 速查表

| 层级 | 方法 | 说明 |
|------|------|------|
| **OrderBook** | `from_asset(market, symbol, ...)` | 通过市场+品种创建 |
| | `build_checkpoints(n_days, force)` | 构建检查点 |
| | `snapshot(ts)` | 分钟/秒级快照查询 |
| | `snapshot_range(start, end)` | 范围查询 |
| | `best_bid()` / `best_ask()` | 最优买卖价 |
| | `top_bids(n)` / `top_asks(n)` | Top N 档 |
| | `crossover()` | 当前 crossover 状态 |
| **list_assets** | `list_assets(market)` | 列出可用 Binance 资产 |
| | `snapshot_events` | 当日快照生效的审计记录 |
| **StateMachine** | `process(ts, is_bid, price, amount, is_snapshot=False)` | 单条处理 |
| | `process_batch(ts, bids, px, amt, is_snapshots=None)` | numpy 批量处理 |
| | `flush_snapshot()` | 完成最后一个 local_timestamp 消息 |
| | `begin_file()` | 开始新文件并启用首快照门控 |
| | `set_snapshot_reset_enabled(bool)` | 快照重置语义开关 (A/B) |
| | `snapshot_events` / `clear_snapshot_events()` | 快照事件 |
| | `set_snapshot_enabled(bool)` | 分钟快照开关 |
| | `get_best_bid()` / `get_best_ask()` | 最优价 |
| | `save_checkpoint()` / `load_checkpoint()` | 检查点读写 |

## 测试

```bash
python tests/test_snapshot_semantics.py     # 单元 + 因果 + 回归，退出码 0 = 通过
```

真实因果用例使用配置文件指定的 v6 检查点目录；没有对应日期文件时会自动跳过真实数据部分。
