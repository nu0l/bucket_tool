import logging
import os
import urllib.parse

from src.handlers.base import BucketHandler


class IBMCloudObjectStorageHandler(BucketHandler):
    def __init__(self, bucket_url, session):
        self.bucket_url = bucket_url
        self.session = session

    def list_objects(self, prefix=None):
        all_keys = []
        marker = None

        while True:
            params = {"prefix": prefix or "", "marker": marker or ""}
            response = self.session.get(self.bucket_url, params=params, timeout=10)

            if response.status_code == 200:
                keys = self._parse_response(response.json())
                all_keys.extend(keys)

                # 如果没有下一页，则退出循环
                if not keys:
                    break

                marker = keys[-1][0]
            else:
                self._log_error("Listing objects", self.bucket_url, response.status_code)
                break

        return all_keys

    def _parse_response(self, response_json):
        return [(item['Key'], int(item.get('Size', 0))) for item in response_json.get('Contents', [])]

    def download_object(self, key, local_path):
        file_url = f"{self.bucket_url}/{urllib.parse.quote(key)}"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        with self.session.get(file_url, stream=True, timeout=10) as response:
            if response.status_code == 200:
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            else:
                self._log_error("Download Object", file_url, response.status_code)

    def _log_error(self, action, url, status_code):
        logging.warning(f"[Error] {action} failed for URL: {url}. Status code: {status_code}")
