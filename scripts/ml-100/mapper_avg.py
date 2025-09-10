#!/usr/bin/env python3
# mapper_avg.py
# 用途：从 u.data 中提取 movie_id 与 rating，输出 movie_id\trating
# 供 reducer 计算平均评分。注意：rating 期望是数值型（整数或浮点数）。

import sys


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        # parts[1] 是 movie_id，parts[2] 是 rating
        print(f"{parts[1]}\t{parts[2]}")


if __name__ == '__main__':
    main()
