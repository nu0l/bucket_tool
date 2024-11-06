import logging
import os
import urllib.parse
from xml.etree import ElementTree as ET

from src.handlers.base import BucketHandler


class GoogleGCSHandler(BucketHandler):
    def __init__(self, bucket_url, session, project_id):
        self.bucket_url = f"https://storage.googleapis.com/storage/v1/b/{bucket_url}/o"
        self.session = session
        self.project_id = project_id

    def list_objects(self, prefix=None):
        all_keys = []
        marker = None

        while True:
            params = {"prefix": prefix or "", "pageToken": marker or ""}
            try:
                response = self.session.get(self.bucket_url, params=params, timeout=10)
                if response.headers.get("Content-Type") == "application/xml":
                    # 解析 XML 响应
                    keys, marker = self._parse_xml(response.content)
                elif response.status_code == 200:
                    items = response.json().get('items', [])
                    all_keys.extend([(item['name'], int(item.get('size', 0))) for item in items])
                    marker = response.json().get('nextPageToken')
                else:
                    self._log_error("Listing objects", self.bucket_url, response.status_code)
                    break

                if marker is None:
                    break
            except Exception as e:
                self._log_error("Listing objects", self.bucket_url, e)
                break

        return all_keys

    def download_object(self, key, local_path):
        file_url = f"{self.bucket_url}/{urllib.parse.quote(key)}?alt=media"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        try:
            with self.session.get(file_url, stream=True, timeout=10) as response:
                if response.status_code == 200:
                    with open(local_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                else:
                    self._log_error("Download Object", file_url, response.status_code)
        except Exception as e:
            self._log_error("Download Object", file_url, e)

    def _parse_xml(self, xml_content):
        keys = []
        root = ET.fromstring(xml_content)

        contents = root.findall(".//Contents")

        for content in contents:
            key_element = content.find(".//Key")
            size_element = content.find(".//Size")

            if key_element is not None and size_element is not None:
                key = key_element.text
                size = int(size_element.text)
                keys.append((key, size))

        is_truncated = root.find(".//IsTruncated")
        next_marker = None
        if is_truncated is not None and is_truncated.text == 'true':
            next_marker_element = root.find(".//NextMarker")
            next_marker = next_marker_element.text if next_marker_element is not None else None

        return keys, next_marker

    def _log_error(self, action, url, error):
        logging.warning(f"[Error] {action} failed for URL: {url}. Error: {error}")
