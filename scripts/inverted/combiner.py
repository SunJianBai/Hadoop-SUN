#!/usr/bin/env python3
import sys

def main():
    current_key = None
    current_count = 0
    for line in sys.stdin:
        line = line.strip()
        if not line: continue
        key, val = line.split('\t', 1)
        try:
            cnt = int(val)
        except:
            cnt = 0
        if key == current_key:
            current_count += cnt
        else:
            if current_key is not None:
                # key is word:filename -> we need to convert to word \t filename:count
                kw = current_key
                idx = kw.rfind(':')
                if idx != -1:
                    word = kw[:idx]
                    fname = kw[idx+1:]
                    print(f"{word}\t{fname}:{current_count}")
            current_key = key
            current_count = cnt
    # flush
    if current_key is not None:
        kw = current_key
        idx = kw.rfind(':')
        if idx != -1:
            word = kw[:idx]
            fname = kw[idx+1:]
            print(f"{word}\t{fname}:{current_count}")

if __name__ == '__main__':
    main()
