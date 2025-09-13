import subprocess

# 创建 HDFS 目录 /input/sentences/files
def mkdir_hdfs():
    cmd = ["hdfs", "dfs", "-mkdir", "-p", "/input/sentences/files"]
    try:
        subprocess.run(cmd, check=True)
        print("HDFS 目录创建成功: /input/sentences/files")
    except subprocess.CalledProcessError:
        print("HDFS 目录创建失败或已存在")

if __name__ == "__main__":
    mkdir_hdfs()

