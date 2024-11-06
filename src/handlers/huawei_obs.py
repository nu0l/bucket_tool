import logging
import os
import urllib.parse
import xml.etree.ElementTree as ET

from src.handlers.base import BucketHandler


class HuaweiOBSHandler(BucketHandler):
    def __init__(self, bucket_url, session):
        self.bucket_url = bucket_url
        self.session = session
        self.bucket_name = self.extract_bucket_name(bucket_url)

    def extract_bucket_name(self, bucket_url):
        return bucket_url.split('.')[0].split('//')[-1]

    def list_objects(self, prefix=None):
        all_keys = []
        marker = None

        while True:
            params = {}
            if prefix:
                params['prefix'] = prefix
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
        for contents in root.findall("{http://obs.myhwclouds.com/doc/2015-06-30/}Contents"):
            key = contents.find("{http://obs.myhwclouds.com/doc/2015-06-30/}Key").text
            size = int(contents.find("{http://obs.myhwclouds.com/doc/2015-06-30/}Size").text)
            keys.append((key, size))

        # 检查是否有下一页
        is_truncated = root.find("{http://obs.myhwclouds.com/doc/2015-06-30/}IsTruncated").text == 'true'
        next_marker = root.find("{http://obs.myhwclouds.com/doc/2015-06-30/}NextMarker").text if is_truncated else None
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

    def _log_error(self, action, url, status_code):
        logging.warning(f"[Error] {action} failed for URL: {url}. Status code: {status_code}")