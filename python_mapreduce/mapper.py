#!/usr/bin/env python3
#!/usr/bin/env python3
"""
Mapper for Hadoop Streaming (更稳健的实现，带中文注释)

输入格式（默认假定）：每行以空格分隔，第一列为句子 id 或其它元信息，后续字段为单词。
例如："123 我 爱 自然 语言 处理"

输出格式：
  word:filename\t1

设计说明：
- 把文件名作为 key 的一部分（word:filename）可以把同一文件内的计数先在 map/combiner 端聚合，
  减少 shuffle 数据量。
- 为兼容不同 Hadoop 版本，尝试从多个环境变量读取输入文件名。
"""
import sys
import os
from os import path


def get_filename():
    """尝试从常见环境变量获取当前处理的输入文件名，找不到则返回 'unknown'。"""
    for k in ('mapreduce_map_input_file', 'map_input_file', 'mapreduce_map_input_split'):
        v = os.environ.get(k)
        if v:
            return path.basename(v)
    v = os.environ.get('HADOOP_MAP_INPUT_FILE') or os.environ.get('mapreduce.map.input.file')
    if v:
        return path.basename(v)
    return 'unknown'


def main():
    filename = get_filename()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            # 无足够字段，跳过
            continue
        words = parts[1:]
        for w in words:
            # 直接输出原始 token，不做额外归一化（可按需修改）
            try:
                print(f"{w}:{filename}\t1")
            except Exception:
                # 遇到无法打印的行则继续，保证 mapper 不崩溃
                continue


if __name__ == '__main__':
    main()
