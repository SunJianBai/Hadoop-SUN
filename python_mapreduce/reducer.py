#!/usr/bin/env python3
"""
Reducer for Hadoop Streaming

输入（来自 combiner 或 mapper）示例：
  word\tfilename:count

输出（每个词的一行）示例：
  word\tfile1:cnt1;file2:cnt2;...

说明：此 reducer 会把同一文件的计数合并（若同一 filename 出现多次），
并且会跳过格式异常的记录以保证 reducer 稳定运行。
输出可直接被下游脚本（如 load_to_hbase.sh）解析并写入 HBase。
"""
import sys
from collections import defaultdict


def flush_and_emit(cur_word, counts):
    """将聚合后的 counts （dict filename->count）输出为 word\tfile:cnt;... 格式"""
    if not cur_word:
        return
    parts = [f"{fname}:{cnt}" for fname, cnt in counts.items()]
    print(f"{cur_word}\t" + ';'.join(parts))


def main():
    cur_word = None
    counts = defaultdict(int)  # filename -> total count

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split('\t', 1)
        if len(parts) != 2:
            # 格式异常，忽略此行
            continue
        word, rest = parts
        # rest 期望是 filename:count
        try:
            fname, cnt_str = rest.split(':', 1)
            cnt = int(cnt_str)
        except Exception:
            # 格式或数字异常则跳过
            continue

        if cur_word is None:
            cur_word = word

        if word != cur_word:
            # 当前词切换，输出之前的聚合结果
            flush_and_emit(cur_word, counts)
            # reset
            counts = defaultdict(int)
            cur_word = word

        counts[fname] += cnt

    # 输出最后一组
    if cur_word is not None:
        flush_and_emit(cur_word, counts)


if __name__ == '__main__':
    main()
