#!/usr/bin/env python3
"""
Combiner for Hadoop Streaming

输入（来自 mapper）示例：
  word:filename\t1

输出（供 reducer）示例：
  word\tfilename:count

此版本增强了对异常行的容错能力，避免单条错误导致整个任务失败。
"""
import sys


def emit(word, fname, count):
    print(f"{word}\t{fname}:{count}")


def main():
    cur_key = None
    cur_sum = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split('\t', 1)
        if len(parts) != 2:
            # 格式不符，跳过
            continue
        k, v = parts
        try:
            v_int = int(v)
        except Exception:
            # 非数字值忽略
            continue

        if k == cur_key:
            cur_sum += v_int
        else:
            if cur_key is not None:
                try:
                    word, fname = cur_key.split(':', 1)
                    emit(word, fname, cur_sum)
                except ValueError:
                    # key 格式异常，跳过
                    pass
            cur_key = k
            cur_sum = v_int

    # 输出最后一组
    if cur_key is not None:
        try:
            word, fname = cur_key.split(':', 1)
            emit(word, fname, cur_sum)
        except ValueError:
            pass


if __name__ == '__main__':
    main()
