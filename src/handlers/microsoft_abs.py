import logging
import os
import urllib.parse
import xml.etree.ElementTree as ET

from src.handlers.base import BucketHandler


class MicrosoftAzureBlobStorageHandler(BucketHandler):
    def __init__(self, bucket_url: str, session) -> None:
        self.bucket_url = f"{bucket_url}?restype=container&comp=list"
        self.session = session

    def list_objects(self, prefix: str = None) -> list:
        all_keys = []
        marker = None

        while True:
            params = {"prefix": prefix or ""}
            if marker:
                params['marker'] = marker

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

        for blob in root.findall(".//{*}Blob"):
            key = blob.find("{*}Name").text
            size = int(blob.find("{*}Properties/{*}Content-Length").text)
            keys.append((key, size))

        next_marker = root.find(".//{*}NextMarker")
        return keys, next_marker.text if next_marker is not None else None

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
                logging.info(f"Downloaded {key} to {local_path}")

        except Exception as e:
            self._log_error("Downloading object", file_url, e)

    def _log_error(self, action: str, url: str, error: Exception) -> None:
        logging.warning(f"[Error] {action} failed for URL: {url}. Error: {str(error)}")
