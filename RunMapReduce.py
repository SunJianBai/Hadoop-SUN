#!/usr/bin/env python3
"""Local runner to simulate Hadoop Streaming for the inverted index example.

Usage: python RunMapReduce.py [--no-combiner]
This script reads files under data/sentences/files and runs mapper -> combiner -> reducer
using the scripts in scripts/inverted/.
"""
import subprocess
import sys
import glob
import os
import tempfile

ROOT = os.path.dirname(__file__)
IN_DIR = os.path.join(ROOT, 'data', 'sentences', 'files')
MAPPER = os.path.join(ROOT, 'scripts', 'inverted', 'mapper.py')
COMBINER = os.path.join(ROOT, 'scripts', 'inverted', 'combiner.py')
REDUCER = os.path.join(ROOT, 'scripts', 'inverted', 'reducer.py')

def run_mapper_all():
    files = sorted(glob.glob(os.path.join(IN_DIR, '*')))
    if not files:
        print('No input files under', IN_DIR)
        return None
    # Run mapper on each file, capture outputs in a temp file
    tmp = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
    for f in files:
        with open(f, 'r', encoding='utf-8') as fh:
            p = subprocess.Popen([sys.executable, MAPPER], stdin=fh, stdout=subprocess.PIPE, env={**os.environ, 'mapreduce_map_input_file': f})
            out, _ = p.communicate()
            if out:
                tmp.write(out.decode() if isinstance(out, bytes) else out)
    tmp.flush(); tmp.close()
    return tmp.name

def sort_and_group(input_path):
    with open(input_path, 'r', encoding='utf-8') as fh:
        lines = [l for l in fh]
    lines.sort()
    tmp = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
    for l in lines:
        tmp.write(l)
    tmp.flush(); tmp.close()
    return tmp.name

def run_combiner(sorted_path):
    tmp = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
    with open(sorted_path, 'r', encoding='utf-8') as fh:
        p = subprocess.Popen([sys.executable, COMBINER], stdin=fh, stdout=subprocess.PIPE)
        out, _ = p.communicate()
        if out:
            tmp.write(out.decode() if isinstance(out, bytes) else out)
    tmp.flush(); tmp.close()
    return tmp.name

def run_reducer(input_path):
    with open(input_path, 'r', encoding='utf-8') as fh:
        p = subprocess.Popen([sys.executable, REDUCER], stdin=fh, stdout=subprocess.PIPE)
        out, _ = p.communicate()
        if out:
            print(out.decode() if isinstance(out, bytes) else out)

def main():
    no_combiner = '--no-combiner' in sys.argv
    print('Running mapper over input files...')
    mapped = run_mapper_all()
    if not mapped:
        return
    print('Sorting mapper output...')
    sorted_m = sort_and_group(mapped)
    if not no_combiner:
        print('Running combiner...')
        combined = run_combiner(sorted_m)
        # combiner output should be sorted already
        print('Running reducer...')
        run_reducer(combined)
    else:
        print('Running reducer directly...')
        run_reducer(sorted_m)

if __name__ == '__main__':
    main()
