import logging
import os
import urllib.parse

from src.handlers.base import BucketHandler


class BackblazeB2Handler(BucketHandler):
    def __init__(self, bucket_url, session, auth_token):
        self.bucket_url = bucket_url
        self.session = session
        self.auth_token = auth_token

    def list_objects(self, prefix=None):
        all_files = []
        start_file_name = None

        while True:
            headers = {"Authorization": self.auth_token}
            params = {"prefix": prefix or "", "startFileName": start_file_name or ""}
            response = self.session.get(self.bucket_url, headers=headers, params=params, timeout=10)

            if response.status_code == 200:
                files = response.json().get('files', [])
                all_files.extend([(item['fileName'], int(item['size'])) for item in files])

                if not files:
                    break

                start_file_name = files[-1]['fileName']
            else:
                self._log_error("Listing objects", self.bucket_url, response.status_code)
                break

        return all_files

    def download_object(self, key, local_path):
        file_url = f"{self.bucket_url}/{urllib.parse.quote(key)}"
        headers = {"Authorization": self.auth_token}
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        with self.session.get(file_url, headers=headers, stream=True, timeout=10) as response:
            if response.status_code == 200:
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            else:
                self._log_error("Download object", file_url, response.status_code)

    def _log_error(self, action, url, status_code):
        logging.warning(f"[Error] {action} failed for URL: {url}. Status code: {status_code}")
