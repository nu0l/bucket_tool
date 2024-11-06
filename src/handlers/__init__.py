import requests
from .aliyun_oss import AliyunOSSHandler
from .huawei_obs import HuaweiOBSHandler
from .tencent_cos import TencentCOSHandler
from .amazon_s3 import AmazonS3Handler
from .backblaze_b2 import BackblazeB2Handler
from .digitalocean_spaces import DigitalOceanSpacesHandler
from .google_gcs import GoogleGCSHandler
from .ibm_cos import IBMCloudObjectStorageHandler
from .microsoft_abs import MicrosoftAzureBlobStorageHandler
from .oracle_ocs import OracleCloudStorageHandler

class BucketFactory:
    HANDLERS = {
        "ali": AliyunOSSHandler,
        "hw": HuaweiOBSHandler,
        "tx": TencentCOSHandler,
        "s3": AmazonS3Handler,
        "b2": BackblazeB2Handler,
        "do": DigitalOceanSpacesHandler,
        "gcs": GoogleGCSHandler,
        "ibm": IBMCloudObjectStorageHandler,
        "abs": MicrosoftAzureBlobStorageHandler,
        "oci": OracleCloudStorageHandler,
    }

    @staticmethod
    def get_handler(bucket_url: str, session: requests.Session, module: str):
        handler_class = BucketFactory.HANDLERS.get(module)
        return handler_class(bucket_url, session) if handler_class else None

    @staticmethod
    def get_supported_modules():
        return list(BucketFactory.HANDLERS.keys())
