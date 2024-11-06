import argparse
import logging
import signal
from typing import List


def read_urls_from_file(file_path: str) -> List[str]:
    with open(file_path, 'r') as file:
        return [line.strip().rstrip('/') for line in file if line.strip()]


def configure_logging():
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


def handle_sigint():
    def signal_handler(sig, frame):
        logging.info("\nProcess interrupted by user. Exiting gracefully...")
        exit(0)

    signal.signal(signal.SIGINT, signal_handler)


def print_logo():
    logo = """
      ______________
     |              |
     | Cloud Bucket |
     |     Tool     |
     |______________|
    """
    print(logo)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Cloud Storage Bucket Traversal Tool")
    parser.add_argument("-u", "--url", help="Single bucket URL")
    parser.add_argument("-f", "--file", help="File containing bucket URLs")
    parser.add_argument("-p", "--proxy", help="Set global proxy (e.g., http://127.0.0.1:8080)")
    parser.add_argument("-t", "--threads", type=int, default=3, help="Number of threads to use for downloading")

    # 定义支持的模块和描述
    module_help = {
        "hw": "Huawei",
        "ali": "Aliyun",
        "tx": "Tencent",
        "s3": "Amazon S3",
        "b2": "Backblaze B2",
        "do": "DigitalOcean Spaces",
        "gcs": "Google Cloud Storage",
        "ibm": "IBM Cloud Object Storage",
        "abs": "Microsoft Azure Blob Storage",
        "oci": "Oracle Cloud Storage"
    }

    # 生成帮助字符串
    module_descriptions = ", ".join(f"{key} for {value}" for key, value in module_help.items())
    parser.add_argument("-m", "--module", required=True,
                        help=f"Cloud module ({module_descriptions})")

    args = parser.parse_args()

    # 检查必要参数
    if not args.url and not args.file:
        parser.error("Either -u (URL) or -f (file) must be provided.")
    if not args.module:
        parser.error("The -m (module) parameter is required.")

    # 检查模块有效性
    valid_modules = module_help.keys()
    if args.module not in valid_modules:
        parser.error(f"Invalid module '{args.module}'. Supported modules are: {', '.join(valid_modules)}.")

    return args


def validate_module(bucket_url, module):
    """Validate the bucket URL against the specified module."""
    module_keywords = {
        "hw": "obs",  # 华为云对象存储
        "ali": "oss",  # 阿里云对象存储
        "tx": "cos",  # 腾讯云对象存储
        "s3": "s3",  # 亚马逊S3
        "b2": "b2",  # Backblaze B2
        "do": "spaces",  # DigitalOcean Spaces
        "gcs": "gcs",  # Google云存储
        "ibm": "cos",  # IBM云对象存储
        "abs": "blob",  # Microsoft Azure Blob Storage
        "oci": "ocs"  # Oracle云存储
    }

    # 检查模块是否有效
    if module not in module_keywords:
        logging.error(f"Invalid module '{module}' provided. Supported modules are: {', '.join(module_keywords.keys())}.")
        return False

    expected_keyword = module_keywords[module]
    if expected_keyword not in bucket_url:
        logging.error(
            f"URL does not match {module} module. Expected keyword '{expected_keyword}' not found in the URL.")
        return False

    return True

