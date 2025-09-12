#!/usr/bin/env python3
import sys

def main():
    current_word = None
    file_entries = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        word, rest = line.split('\t', 1)
        if word != current_word:
            if current_word is not None:
                # output: word \t file1:count;file2:count;...
                print(f"{current_word}\t{';'.join(file_entries)}")
            current_word = word
            file_entries = [rest]
        else:
            file_entries.append(rest)
    if current_word is not None:
        print(f"{current_word}\t{';'.join(file_entries)}")

if __name__ == '__main__':
    main()
