from src.handlers.ibm_cos import IBMCloudObjectStorageHandler


class OracleCloudStorageHandler(IBMCloudObjectStorageHandler):
    """
    OracleCloudStorageHandler 继承自 IBMCloudObjectStorageHandler。
    Oracle Cloud 的对象存储接口类似于 AWS S3 和 IBM COS，
    因此可以直接复用 IBMCloudObjectStorageHandler 的逻辑。
    """
    pass
