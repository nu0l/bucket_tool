import logging
from abc import ABC, abstractmethod

class BucketHandler(ABC):
    @abstractmethod
    def list_objects(self, prefix=None, marker=None):
        pass

    @abstractmethod
    def download_object(self, key, local_path):
        pass

    def _log_error(self, action, url, status_code):
        logging.warning(f"[Error] {action} failed for URL: {url}. Status code: {status_code}")
