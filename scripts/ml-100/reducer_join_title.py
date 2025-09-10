#!/usr/bin/env python3
# reducer_join_title.py
# 用途：把 movie_id 替换成对应的 movie title。这个 reducer 在启动时会把本地的
# u.item 文件（movie_id|title|...）读入内存为字典，然后处理 STDIN 中的 movie_id\t... 行，
# 输出 title\t... 。
# 使用方法：在 hadoop streaming 命令中增加 -files data/ml-100k/ml-100k/u.item
# 或 -file 将 u.item 分发到每个 reducer 的工作目录，这样脚本就能直接打开本地文件 'u.item'。

import sys
import os


def load_titles(path='u.item'):
    # 返回 movie_id -> title 的 dict
    m = {}
    if not os.path.exists(path):
        # 若文件不存在，返回空映射（则会原样输出 movie_id）
        return m
    # u.item 可能包含非 UTF-8 字符（旧数据集），使用 latin-1 可避免解码错误
    with open(path, encoding='latin-1', errors='ignore') as f:
        for line in f:
            parts = line.strip().split('|')
            if len(parts) >= 2:
                m[parts[0]] = parts[1]
    return m


def main():
    title_map = load_titles('u.item')
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split('\t')
        mid = parts[0]
        rest = '\t'.join(parts[1:]) if len(parts) > 1 else ''
        title = title_map.get(mid, mid)
        print(f"{title}\t{rest}")


if __name__ == '__main__':
    main()
