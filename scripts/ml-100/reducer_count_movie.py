#!/usr/bin/env python3
# reducer_count_movie.py
# 用途：Hadoop Streaming 的 reducer 脚本。接收 mapper 输出的 movie_id\t1，
# 把相同 movie_id 的计数累加，最终输出 movie_id\tcount。
# 这个 reducer 在本地会把所有键读入内存并排序后输出（适合小数据集）。

import sys
from collections import defaultdict


def main():
    # 使用 dict 存储每个 movie_id 的计数
    cnt = defaultdict(int)
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        # mapper 输出以制表符分隔 key 和 value
        parts = line.split('\t')
        if len(parts) != 2:
            # 如果格式不对，跳过该行
            continue
        k, v = parts
        try:
            cnt[k] += int(v)
        except ValueError:
            # 如果 value 不是整型，忽略
            continue

    # 按计数降序输出结果，便于直接查看 top N（注意：在真正的大数据场景中
    # 不建议把所有键载入内存，这里是为了教学与小数据集方便展示）
    for k in sorted(cnt, key=lambda x: -cnt[x]):
        print(f"{k}\t{cnt[k]}")


if __name__ == '__main__':
    main()
