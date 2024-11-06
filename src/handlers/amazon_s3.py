import logging
import os
import xml.etree.ElementTree as ET

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError, ReadTimeoutError

from src.handlers.base import BucketHandler


class AmazonS3Handler(BucketHandler):
    def __init__(self, bucket_name, aws_access_key_id, aws_secret_access_key, region_name="us-east-1"):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        self.bucket_name = bucket_name

    def list_objects(self, prefix=None):
        all_keys = []
        continuation_token = None

        while True:
            try:
                kwargs = {
                    'Bucket': self.bucket_name,
                    'Prefix': prefix or "",
                }
                if continuation_token:
                    kwargs['ContinuationToken'] = continuation_token

                response = self.s3.list_objects_v2(**kwargs)
                if response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
                    # XML response
                    xml_content = response['Body'].read()
                    keys, _ = self._parse_xml(xml_content)
                    all_keys.extend(keys)

                    # 检查是否有下一页
                    if not response.get('IsTruncated'):
                        break

                    continuation_token = response.get('NextContinuationToken')
                else:
                    self._log_error("Listing objects", f"s3://{self.bucket_name}/{prefix}", response)
                    break
            except (ClientError, EndpointConnectionError, ReadTimeoutError) as e:
                self._log_error("Listing objects", f"s3://{self.bucket_name}/{prefix}", e)
                break

        return all_keys

    def _parse_xml(self, xml_content):
        keys = []
        root = ET.fromstring(xml_content)

        contents = root.findall("{http://s3.amazonaws.com/doc/2006-03-01/}Contents")
        if not contents:
            contents = root.findall("Contents")

        for content in contents:
            key_element = content.find("{http://s3.amazonaws.com/doc/2006-03-01/}Key")
            size_element = content.find("{http://s3.amazonaws.com/doc/2006-03-01/}Size")
            if not key_element or not size_element:
                key_element = content.find("Key")
                size_element = content.find("Size")

            if key_element is not None and size_element is not None:
                key = key_element.text
                size = int(size_element.text)
                keys.append((key, size))

        # 检查是否有下一页
        is_truncated = root.find("{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated")
        if is_truncated is None:
            is_truncated = root.find("IsTruncated")

        next_marker = None
        if is_truncated is not None and is_truncated.text == 'true':
            next_marker = root.find("{http://s3.amazonaws.com/doc/2006-03-01/}NextMarker")
            if next_marker is None:
                next_marker = root.find("NextMarker")

        return keys, next_marker.text if next_marker is not None else None

    def download_object(self, key, local_path):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        try:
            self.s3.download_file(self.bucket_name, key, local_path)
        except (ClientError, EndpointConnectionError, ReadTimeoutError) as e:
            self._log_error("Download object", f"s3://{self.bucket_name}/{key}", e)

    def _log_error(self, action, url, error):
        logging.warning(f"[Error] {action} failed for URL: {url}. Error: {error}")
