#!/usr/bin/env python3
"""Simple Flask web UI to query HBase via the existing Java jar.

Usage:
  pip install -r requirements.txt
  python3 app.py

Open http://localhost:5000/ and search.
"""
from flask import Flask, render_template, request
import subprocess
import os

app = Flask(__name__, template_folder='templates', static_folder='static')


def run_query(word):
    """Run the Java HBaseReaderQuery jar and parse output.

    Returns a dict with keys: error | notfound | row | results | raw
    """
    # locate jar relative to repo
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    jar = os.path.join(base, 'HBaseReaderQuery', 'target', 'HBaseReaderQuery-1.0-SNAPSHOT.jar')
    if not os.path.exists(jar):
        return {'error': f'Jar not found at {jar}. 请先构建 HBaseReaderQuery 项目（mvn/gradle）。'}

    # get HBase classpath
    try:
        cp = subprocess.check_output(['hbase', 'classpath'], stderr=subprocess.STDOUT).decode().strip()
    except Exception as e:
        return {'error': f'无法获取 HBase classpath：{e}。确保 hbase 在 PATH 并可执行 `hbase classpath`。'}

    cmd = ['java', '-cp', f'{jar}:{cp}', 'com.test.HBaseReaderQuery', 'InvertedIndexTable', word]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('utf-8', errors='replace')
    except subprocess.CalledProcessError as e:
        return {'error': f'查询命令返回错误，退出码 {e.returncode}，输出：\n{e.output.decode("utf-8",errors="replace")}' }
    except Exception as e:
        return {'error': f'运行查询时发生异常：{e}'}

    # parse output
    if 'not found' in out:
        return {'notfound': True, 'raw': out}

    row = None
    vals = None
    for line in out.splitlines():
        line = line.strip()
        if line.startswith('RowKey'):
            parts = line.split('=', 1)
            if len(parts) > 1:
                row = parts[1].strip()
        elif line.startswith('Value'):
            parts = line.split('=', 1)
            if len(parts) > 1:
                vals = parts[1].strip()

    results = []
    if vals:
        # expect format file:count;file2:count2;...
        for part in vals.split(';'):
            part = part.strip()
            if not part:
                continue
            if ':' in part:
                f, c = part.split(':', 1)
            else:
                f, c = part, ''
            results.append({'file': f, 'count': c})

    return {'row': row, 'results': results, 'raw': out}


@app.route('/', methods=['GET', 'POST'])
def index():
    query = ''
    data = None
    error = None
    if request.method == 'POST':
        query = request.form.get('q', '').strip()
        if not query:
            error = '请输入查询单词'
        else:
            data = run_query(query)
            if isinstance(data, dict) and 'error' in data:
                error = data['error']
    return render_template('index.html', query=query, data=data, error=error)


@app.route('/view')
def view_file():
    """View a file from HDFS under the configured HDFS_DIR. Limits lines to avoid huge responses."""
    fname = request.args.get('file', '')
    # basic validation: no path traversal, only basename
    if not fname or '/' in fname or '\\' in fname:
        return "非法文件名", 400

    # hdfs path
    HDFS_DIR = "/input/sentences/files"
    hdfs_path = f"{HDFS_DIR}/{fname}"

    # run hdfs cat but limit output lines
    try:
        # Use shell pipeline to limit output on the hdfs client side and avoid SIGPIPE errors
        max_lines = 200
        cmd = f"hdfs dfs -cat '{hdfs_path}' | head -n {max_lines}"
        out = subprocess.check_output(['bash', '-lc', cmd], stderr=subprocess.STDOUT, text=True)
        out_lines = out.splitlines()
    except subprocess.CalledProcessError as e:
        # hdfs client returned non-zero, include its stderr/stdout
        return render_template('view.html', file=fname, error=e.output or '读取 HDFS 文件失败', lines=None)
    except Exception as e:
        return render_template('view.html', file=fname, error=str(e), lines=None)

    return render_template('view.html', file=fname, lines=out_lines)


if __name__ == '__main__':
    # debug mode for development; in production use a WSGI server
    app.run(host='0.0.0.0', port=5000, debug=True)
