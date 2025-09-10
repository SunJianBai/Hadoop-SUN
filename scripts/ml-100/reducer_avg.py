#!/usr/bin/env python3
# reducer_avg.py
# 用途：接收来自 mapper 的 movie_id\trating，计算每个 movie 的平均评分与评分次数，
# 输出格式为 movie_id\tavg_rating\tcount。输出按平均评分降序排列，便于查看高评分电影。

import sys
from collections import defaultdict


def main():
    sumv = defaultdict(float)
    cnt = defaultdict(int)
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split('\t')
        if len(parts) != 2:
            continue
        k, v = parts
        try:
            r = float(v)
        except ValueError:
            # 如果 rating 不能转换为 float，忽略该条记录
            continue
        sumv[k] += r
        cnt[k] += 1

    # 按平均评分降序输出。注意如果 count 较小，平均值可能不可靠。
    for k in sorted(sumv, key=lambda x: - (sumv[x]/cnt[x] if cnt[x] else 0)):
        avg = sumv[k]/cnt[k] if cnt[k] else 0
        print(f"{k}\t{avg:.3f}\t{cnt[k]}")


if __name__ == '__main__':
    main()
