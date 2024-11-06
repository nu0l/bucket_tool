from src.handlers.ibm_cos import IBMCloudObjectStorageHandler


class DigitalOceanSpacesHandler(IBMCloudObjectStorageHandler):
    """
    DigitalOceanSpacesHandler 继承自 IBMCloudObjectStorageHandler。
    DigitalOcean Spaces 接口完全兼容 S3 API，因此可以直接使用 IBMCloudObjectStorageHandler 的逻辑。
    该设计便于未来扩展 DigitalOcean 特有的逻辑。
    """
    pass