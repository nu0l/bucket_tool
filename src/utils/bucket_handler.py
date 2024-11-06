import logging
import os

import requests
from tqdm import tqdm

from src.handlers import BucketFactory
from src.utils.downloader import download_files, log_download_stats
from src.utils.helpers import validate_module


def process_buckets(bucket_urls, module, session, threads):
    """处理每个存储桶，统计文件总大小和数量，下载文件。"""
    for bucket_url in bucket_urls:
        bucket_name = bucket_url.split("//")[1].split(".")[0]

        # URL 和模块类型匹配校验
        if not validate_module(bucket_url, module):
            continue

        log_dir = f"log/{module}/{bucket_name}"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "downloads.log")

        # 生成存储桶处理器实例
        bucket_handler = BucketFactory.get_handler(bucket_url, session, module)

        try:
            keys = bucket_handler.list_objects()
        except requests.exceptions.ReadTimeout:
            logging.error(f"Timeout while trying to access {bucket_url}. Please check your connection and try again.")
            continue
        except Exception as e:
            logging.error(f"An error occurred while accessing {bucket_url}: {str(e)}")
            continue

        if keys:
            total_size, file_count, file_formats = calculate_stats(keys)

            # 控制台输出
            logging.info(f"Bucket: {bucket_name}")
            logging.info(f"Total files: {file_count}")
            logging.info(f"Total size: {total_size / (1024 * 1024):.2f} MB")
            logging.info(f"File formats: {', '.join(file_formats) or 'No extensions'}")

            # 写入日志文件
            log_download_stats(file_count, total_size, [f"{bucket_url}/{key}" for key, _ in keys], log_file)

            # 下载文件并显示进度条
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading from {bucket_name}") as pbar:
                download_files(bucket_handler, bucket_url, bucket_name, pbar, threads)
        else:
            logging.warning(f"No files found in bucket {bucket_url}")

def calculate_stats(keys):
    """Calculate total size, file count, and file formats."""
    total_size = sum(size for _, size in keys)
    file_count = len(keys)
    file_formats = set(os.path.splitext(key)[1] for key, _ in keys)
    return total_size, file_count, file_formats
