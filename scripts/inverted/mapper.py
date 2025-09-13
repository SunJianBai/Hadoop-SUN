#!/usr/bin/env python3
import sys
import os

def get_input_filename():
    # Hadoop streaming may set different env vars; try common ones
    return os.environ.get('mapreduce_map_input_file') or os.environ.get('map_input_file') \
           or os.environ.get('MAP_INPUT_FILE') or os.environ.get('MAPREDUCE_MAP_INPUT_FILE') \
           or os.environ.get('mapreduce.job.input.path') or ''


def main():
    fname = os.path.basename(get_input_filename()) or 'stdin'
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        words = parts[1:]
        for w in words:
            if w:
                print(f"{w}:{fname}\t1")


if __name__ == '__main__':
    main()
