import boto3.session
import botocore.config
import pyarrow.parquet as pq
import s3fs
from time import time
from arro3.io import read_parquet_async

fs = s3fs.S3FileSystem(anon=True, asynchronous=True)

path = "s3://ookla-open-data/parquet/performance/type=mobile/year=2019/quarter=1/2019-01-01_performance_mobile_tiles.parquet"

path = "s3://ookla-open-data/parquet/performance/type=mobile/year=2019/quarter=1/"
metas = await fs._ls(path, detail=True)
metas

    # pub location: Path,
    # pub last_modified: DateTime<Utc>,
    # pub size: usize,
    # pub e_tag: Option<String>,
    # pub version: Option<String>,


fs._rm


start = time()
info = await fs._info(path)
table = await read_parquet_async(path, fs, info["size"])
end = time()
print(f"time: {end - start:.2f}")
# 8.88
# 6.69
# 10.14
# 10.80
# 11.1
# 8.83

%time pa_table = pq.read_table(path)
# 15.8
# 7.18
# 24.6
# 16.1
# 14.5

import boto3
import botocore

session = boto3.Session()
credentials = session.get_credentials()
credentials =  credentials.get_frozen_credentials()
credentials.access_key
credentials.secret_key
credentials.token
credentials.

s3 = boto3.client("s3")
config: botocore.config.Config = s3._client_config
config.
dir(s3)
c
