import subprocess
import os
import glob
from math import ceil
try:
    from tqdm import tqdm
except Exception:
    tqdm = None
import logging
import sys

# 本地分割文件目录
LOCAL_DIR = "data/sentences/files"
# HDFS目标目录
HDFS_DIR = "/input/sentences/files"
# 检查的文件名
CHECK_FILE = "file0.txt"


def setup_logging(verbose: bool = False):
    logger = logging.getLogger("upload_hdfs")
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s: %(message)s", "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(fmt)
    logger.handlers = [handler]
    return logger


def run_cmd(cmd, logger, check=True, max_output_lines=20):
    """Run command and capture output; return (returncode, stdout, stderr).

    To avoid noisy logs when uploading many files, cap the number of lines shown
    from stdout/stderr unless verbose mode is enabled.
    """
    logger.debug("运行命令: %s", " ".join(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = p.communicate()
    if out:
        lines = out.strip().splitlines()
        if logger.level <= logging.DEBUG:
            logger.debug("stdout: %s", out.strip())
        else:
            snippet = "\n".join(lines[:max_output_lines])
            logger.info("stdout (showing first %d lines):\n%s", min(len(lines), max_output_lines), snippet)
    if err:
        lines = err.strip().splitlines()
        if logger.level <= logging.DEBUG:
            logger.debug("stderr: %s", err.strip())
        else:
            snippet = "\n".join(lines[:max_output_lines])
            logger.warning("stderr (showing first %d lines):\n%s", min(len(lines), max_output_lines), snippet)
    if check and p.returncode != 0:
        logger.error("命令失败，返回码 %d", p.returncode)
    return p.returncode, out, err


def upload_files(logger, verbose=False):
    pattern = os.path.join(LOCAL_DIR, "*")
    files = sorted(glob.glob(pattern))
    if not files:
        logger.error("本地目录没有找到要上传的文件 -> %s", pattern)
        logger.info("请确认相对路径是否正确或先运行数据生成脚本。")
        return False

    total = len(files)
    logger.info("开始上传 %d 个文件到 %s", total, HDFS_DIR)

    iterator = range(total)
    if tqdm is not None:
        iterator = tqdm(range(total), desc="上传进度", unit="file")

    for idx in iterator:
        f = files[idx]
        hdfs_target = f"{HDFS_DIR}/{os.path.basename(f)}"
        # 每个文件先检查是否存在（简短），若需要覆盖可在外部用 --overwrite 清空目录
        cmd_test = ["hdfs", "dfs", "-test", "-e", hdfs_target]
        rc_test, _, _ = run_cmd(cmd_test, logger, check=False)
        if rc_test == 0:
            # 文件已存在
            status = "exists"
            logger.debug("文件已存在：%s", hdfs_target)
        else:
            # 上传单个文件
            rc_put, out, err = run_cmd(["hdfs", "dfs", "-put", f, HDFS_DIR], logger, check=False)
            status = "ok" if rc_put == 0 else "fail"

        # 每 20 个文件输出一次当前状态（简短）
        if (idx + 1) % 20 == 0 or (idx + 1) == total:
            logger.info("已处理 %d/%d 文件. 最近文件: %s 状态=%s", idx + 1, total, os.path.basename(f), status)

        # 避免 tqdm 里重复输出太多 DEBUG 信息
        if not verbose:
            # 抑制每文件的 DEBUG 输出（run_cmd 已根据 logger level 控制详细输出）
            pass

    logger.info("上传过程完成（可能部分文件已存在或失败），目标目录：%s", HDFS_DIR)
    return True


def check_file(logger):
    hdfs_file = f"{HDFS_DIR}/{CHECK_FILE}"
    cmd_test = ["hdfs", "dfs", "-test", "-e", hdfs_file]
    rc, _, _ = run_cmd(cmd_test, logger, check=False)
    if rc == 0:
        logger.info("%s 存在，正在打印内容：", hdfs_file)
        cmd_cat = ["hdfs", "dfs", "-cat", hdfs_file]
        run_cmd(cmd_cat, logger, check=False)
    else:
        logger.warning("%s 不存在", hdfs_file)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Upload local split files to HDFS")
    parser.add_argument("--overwrite", action="store_true", help="overwrite existing files in HDFS")
    parser.add_argument("--verbose", action="store_true", help="show debug logs")
    args = parser.parse_args()

    logger = setup_logging(verbose=args.verbose)

    # if overwrite requested, create HDFS dir and remove existing contents
    if args.overwrite:
        logger.info("--overwrite enabled: will remove existing files under %s before upload", HDFS_DIR)
        run_cmd(["hdfs", "dfs", "-rm", "-r", "-f", HDFS_DIR], logger, check=False)
        run_cmd(["hdfs", "dfs", "-mkdir", "-p", HDFS_DIR], logger, check=False)

    ok = upload_files(logger)
    if ok:
        check_file(logger)

