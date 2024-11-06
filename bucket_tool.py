import logging
# 忽略 InsecureRequestWarning 警告
import warnings

import requests
from urllib3.exceptions import InsecureRequestWarning

from src.utils.bucket_handler import process_buckets
from src.utils.helpers import read_urls_from_file, configure_logging, handle_sigint, print_logo, parse_arguments

warnings.simplefilter('ignore', InsecureRequestWarning)

def main():
    handle_sigint()

    try:
        args = parse_arguments()
        session = requests.Session()
        if args.proxy:
            session.proxies = {
                'http': args.proxy,
                'https': args.proxy,
                'socks5': args.proxy
            }
            session.trust_env = False  # 忽略系统代理设置
        session.verify = False

        bucket_urls = []
        if args.url:
            bucket_urls.append(args.url)
        elif args.file:
            bucket_urls = read_urls_from_file(args.file)

        if not bucket_urls:
            logging.error("No bucket URLs provided. Use -u or -f to specify URLs.")
            return

        process_buckets(bucket_urls, args.module, session, args.threads)

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    print_logo()
    configure_logging()
    main()