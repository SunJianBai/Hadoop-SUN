#!/usr/bin/env python3
# mapper_count_movie.py
# 用途：Hadoop Streaming 的 mapper 脚本，读取 ml-100k 的 u.data（每行：user_id\titem_id\trating\ttimestamp）
# 输出格式：movie_id\t1，用于后续 reducer 对同一 movie_id 求和得到被评分次数。
# 说明：Streaming 会把输入逐行通过 STDIN 传给此脚本，脚本把结果通过 STDOUT 输出。

import sys


def main():
    # 逐行读取来自 STDIN 的记录
    for line in sys.stdin:
        # 去掉行尾换行与首尾空白
        line = line.strip()
        if not line:
            continue
        # u.data 的字段通常使用制表符或空白分隔，split() 足以分割
        parts = line.split()
        # 期望至少包含 user, item, rating 三列
        if len(parts) < 3:
            continue
        # parts[1] 是 movie id（item_id），输出 movie_id\t1 供 reducer 聚合计数
        print(f"{parts[1]}\t1")


if __name__ == '__main__':
    main()
