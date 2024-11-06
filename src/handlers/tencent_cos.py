import logging
import os
import urllib.parse
import xml.etree.ElementTree as ET

from src.handlers.base import BucketHandler


class TencentCOSHandler(BucketHandler):
    def __init__(self, bucket_url: str, session) -> None:
        self.bucket_url = bucket_url
        self.session = session
        self.bucket_name = self.extract_bucket_name(bucket_url)

    def extract_bucket_name(self, bucket_url: str) -> str:
        return bucket_url.split('.')[0].split('//')[-1]

    def list_objects(self, prefix: str = None) -> list:
        all_keys = []
        marker = None

        while True:
            params = {"prefix": prefix or "", "marker": marker or ""}
            try:
                response = self.session.get(self.bucket_url, params=params, timeout=10)
                response.raise_for_status()
                keys, marker = self._parse_xml(response.content)
                all_keys.extend(keys)

                if marker is None:
                    break
            except Exception as e:
                self._log_error("Listing objects", self.bucket_url, e)
                break

        return all_keys

    def _parse_xml(self, xml_content: bytes) -> tuple:
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

        is_truncated_element = root.find("IsTruncated")
        is_truncated = is_truncated_element is not None and is_truncated_element.text == 'true'

        next_marker_element = root.find("NextMarker")
        next_marker = next_marker_element.text if is_truncated and next_marker_element is not None else None

        return keys, next_marker

    def download_object(self, key: str, local_path: str) -> None:
        file_url = f"{self.bucket_url}/{urllib.parse.quote(key)}"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        try:
            with self.session.get(file_url, stream=True, timeout=10) as response:
                response.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
        except Exception as e:
            self._log_error("Download object", file_url, e)

    def _log_error(self, action: str, url: str, error: Exception) -> None:
        logging.warning(f"[Error] {action} failed for URL: {url}. Error: {str(error)}")
