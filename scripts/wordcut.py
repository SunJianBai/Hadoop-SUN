import os
import math
import logging
# 用于处理老师发的文件
# 这个脚本用于将一个大的txt文件按每10000行分割成多个小文件
# 指定输入文件和输出目录
input_file_path = r"./data/sentences/sentences.txt"  # 输入的txt文件路径
output_directory = r'./data/sentences/files'  # 指定的输出目录路径
logging.basicConfig(level=logging.INFO)
logging.info('Starting file splitting')
# 打开原始txt文件,一共有9397023条句子
with open(input_file_path, 'r', encoding='utf-8') as input_file:
    lines = input_file.readlines()
# 计算总行数和文件数
total_lines = len(lines)
num_files = (total_lines + 9999) // 10000
# 分割文件
for i in range(num_files):
    logging.info(f'Processing file {i + 1}/{num_files}')
    start = i * 10000
    end = min((i + 1) * 10000, total_lines)
    output_filename = os.path.join(output_directory, f'file{i}.txt')

    # 写入分割后的内容到新文件
    with open(output_filename, 'w', encoding='utf-8') as output_file:
        output_file.writelines(lines[start:end])