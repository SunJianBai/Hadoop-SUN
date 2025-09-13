#!/usr/bin/env python3
import argparse
import glob
import logging
import os
import subprocess
import sys
try:
    from tqdm import tqdm
except Exception:
    tqdm = None

LOCAL_DIR = "data/sentences/files"
HDFS_DIR = "/input/sentences/files"
STATUS_EVERY = 20

def setup_logger(verbose: bool):
    logger = logging.getLogger("upload_sample")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s: %(message)s", "%Y-%m-%d %H:%M:%S")
    h.setFormatter(fmt)
    logger.handlers = [h]
    return logger

def run_cmd(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = p.communicate()
    return p.returncode, out.strip(), err.strip()

def upload_sample(count: int, overwrite: bool, verbose: bool):
    logger = setup_logger(verbose)
    files = sorted(glob.glob(os.path.join(LOCAL_DIR, "*")))
    if not files:
        logger.error("本地目录无文件: %s", LOCAL_DIR)
        return 1
    files = files[:count]
    total = len(files)
    logger.info("准备上传 %d 个文件 到 %s", total, HDFS_DIR)

    iterator = range(total)
    if tqdm is not None:
        iterator = tqdm(range(total), desc="上传进度", unit="file")

    for idx in iterator:
        f = files[idx]
        basename = os.path.basename(f)
        hdfs_target = f"{HDFS_DIR}/{basename}"

        # 如果要求覆盖，先删除目标（单文件）
        if overwrite:
            rc, _, err = run_cmd(["hdfs", "dfs", "-rm", "-f", hdfs_target])
            if rc != 0 and err:
                logger.debug("rm stderr: %s", err)

        # 测试是否存在
        rc_test, _, _ = run_cmd(["hdfs", "dfs", "-test", "-e", hdfs_target])
        if rc_test == 0:
            status = "exists"
        else:
            rc_put, out, err = run_cmd(["hdfs", "dfs", "-put", f, HDFS_DIR])
            status = "ok" if rc_put == 0 else "fail"
            if verbose and out:
                logger.debug("put stdout: %s", out)
            if verbose and err:
                logger.debug("put stderr: %s", err)

        # 每 STATUS_EVERY 或最后一次打印一次简短状态
        if (idx + 1) % STATUS_EVERY == 0 or (idx + 1) == total:
            logger.info("已处理 %d/%d 文件. 最近: %s 状态=%s", idx + 1, total, basename, status)

    logger.info("样本上传完成（%d 文件）", total)
    return 0

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=20, help="上传文件数量（默认20）")
    p.add_argument("--overwrite", action="store_true", help="覆盖已存在的 HDFS 文件")
    p.add_argument("--verbose", action="store_true", help="显示调试输出")
    args = p.parse_args()
    sys.exit(upload_sample(args.count, args.overwrite, args.verbose))