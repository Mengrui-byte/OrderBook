#!/usr/bin/env python3
"""Repair crossed v6 checkpoint books without replaying source data."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import struct
import tempfile


HEADER = struct.Struct('<4siiqqi')
ENTRY = struct.Struct('<id')
COUNT = struct.Struct('<q')


def _read_book(path: Path):
    raw = path.read_bytes()
    if len(raw) < HEADER.size + COUNT.size:
        raise ValueError(f'{path}: truncated header')
    magic, version, decimals, timestamp, divisor, flags = HEADER.unpack_from(raw)
    if (magic, version, flags) != (b'OBCK', 6, 15):
        raise ValueError(f'{path}: expected v6 snapshot-aware checkpoint')
    pos = HEADER.size
    bid_count = COUNT.unpack_from(raw, pos)[0]
    pos += COUNT.size
    if bid_count < 0:
        raise ValueError(f'{path}: negative bid count')
    bids = []
    for _ in range(bid_count):
        if pos + ENTRY.size > len(raw):
            raise ValueError(f'{path}: truncated bids')
        bids.append(ENTRY.unpack_from(raw, pos))
        pos += ENTRY.size
    if pos + COUNT.size > len(raw):
        raise ValueError(f'{path}: missing ask count')
    ask_count = COUNT.unpack_from(raw, pos)[0]
    pos += COUNT.size
    if ask_count < 0:
        raise ValueError(f'{path}: negative ask count')
    asks = []
    for _ in range(ask_count):
        if pos + ENTRY.size > len(raw):
            raise ValueError(f'{path}: truncated asks')
        asks.append(ENTRY.unpack_from(raw, pos))
        pos += ENTRY.size
    if pos != len(raw):
        raise ValueError(f'{path}: unexpected trailing bytes')
    return (magic, version, decimals, timestamp, divisor, flags), bids, asks


def _write_book(path: Path, header, bids, asks):
    fd, tmp_name = tempfile.mkstemp(prefix=f'.{path.name}.', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as out:
            out.write(HEADER.pack(*header))
            out.write(COUNT.pack(len(bids)))
            for row in bids:
                out.write(ENTRY.pack(*row))
            out.write(COUNT.pack(len(asks)))
            for row in asks:
                out.write(ENTRY.pack(*row))
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def repair(root: Path, dry_run=False):
    files = sorted(root.glob('*.ckpt'))
    repaired = 0
    removed_bids = removed_asks = 0
    for path in files:
        header, bids, asks = _read_book(path)
        if not bids or not asks or bids[0][0] < asks[0][0]:
            continue
        bid_limit, ask_limit = asks[0][0], bids[0][0]
        new_bids = [row for row in bids if row[0] < bid_limit]
        new_asks = [row for row in asks if row[0] > ask_limit]
        db, da = len(bids) - len(new_bids), len(asks) - len(new_asks)
        if not dry_run:
            _write_book(path, header, new_bids, new_asks)
        repaired += 1
        removed_bids += db
        removed_asks += da
        print(f'{path.name}: removed bids={db}, asks={da}')
    print(f'repaired={repaired} removed_bids={removed_bids} removed_asks={removed_asks}')
    return repaired


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    repair(args.root, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
