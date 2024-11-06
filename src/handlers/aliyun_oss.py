import logging
import os
import urllib.parse
import xml.etree.ElementTree as ET

from src.handlers.base import BucketHandler


class AliyunOSSHandler(BucketHandler):
    def __init__(self, bucket_url, session):
        self.bucket_url = bucket_url
        self.session = session

    def list_objects(self, prefix=None):
        all_keys = []
        marker = None

        while True:
            params = {"prefix": prefix or ""}
            if marker:
                params['marker'] = marker

            response = self.session.get(self.bucket_url, params=params, timeout=10)
            if response.status_code == 200:
                keys, marker = self._parse_xml(response.content)
                all_keys.extend(keys)

                # 如果没有下一页，则退出循环
                if marker is None:
                    break
            else:
                self._log_error("Listing objects", self.bucket_url, response.status_code)
                break

        return all_keys

    def _parse_xml(self, xml_content):
        keys = []
        root = ET.fromstring(xml_content)

        for contents in root.findall("Contents"):
            key_element = contents.find("Key")
            size_element = contents.find("Size")

            if key_element is not None and size_element is not None:
                key = key_element.text
                size = int(size_element.text)
                keys.append((key, size))
            else:
                logging.warning("Key or Size element missing in XML response.")

        # 检查是否有下一页
        is_truncated_element = root.find("IsTruncated")
        is_truncated = is_truncated_element is not None and is_truncated_element.text == 'true'

        next_marker_element = root.find("NextMarker")
        next_marker = next_marker_element.text if is_truncated and next_marker_element is not None else None

        return keys, next_marker

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
                self._log_error("Download object", file_url, response.status_code)

    def _log_error(self, action, url, status_code):
        logging.warning(f"[Error] {action} failed for URL: {url}. Status code: {status_code}")