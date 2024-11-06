import logging
import os
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm


# 下载文件
def download_files(bucket_handler, bucket_url, bucket_name, pbar: tqdm, thread_count: int) -> None:
    keys = bucket_handler.list_objects()
    if not keys:
        logging.warning(f"[-] No files found in bucket {bucket_url}")
        return

    total_size = sum(size for _, size in keys)

    # 定义更新进度条的函数
    def update_progress(size: int) -> None:
        pbar.update(size)

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        for key, size in keys:
            local_path = os.path.join(f"./downloads/{bucket_name}", key)

            # 提交下载任务，并传递回调以更新进度条
            future = executor.submit(bucket_handler.download_object, key, local_path)
            future.add_done_callback(lambda f: update_progress(size))

    pbar.close()

# 记录文件数量、总大小和URL到日志文件
def log_download_stats(file_count: int, total_size: int, file_urls: list, log_file: str) -> None:
    with open(log_file, 'w') as f:
        f.write(f"Total files: {file_count}\n")
        f.write(f"Total size: {total_size / (1024 * 1024):.2f} MB\n")
        f.write("\nFile URLs:\n")
        for url in file_urls:
            f.write(f"{url}\n")
    logging.info(f"Download statistics written to {log_file}")
