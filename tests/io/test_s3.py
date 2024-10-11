import boto3
from arro3.io import S3Store, accept_store, from_url, read_parquet,read_path
from io import BytesIO
import botocore
import botocore.session

session = boto3.Session()
store = S3Store.from_session(session, "ookla-open-data")

url = "s3://ookla-open-data/parquet/performance/type=mobile/year=2019/quarter=1/2019-01-01_performance_mobile_tiles.parquet"
path = "parquet/performance/type=mobile/year=2019/quarter=1/2019-01-01_performance_mobile_tiles.parquet"
buf = await read_path(store, path)
type(buf)
table = read_parquet(BytesIO(buf)).read_all()


session = boto3.Session()
s = botocore.session.Session()
s.region_name
session.get_credentials()
s.get_credentials()
session.re
s3 = session.client("s3")
resource = session.resource("s3")

s3.bucket


store = S3Store.from_boto3_session(session, "ookla-open-data")
accept_store(store)
["session" in s for s in dir(s3)]
test = s3.create_session()


[arro3-io/src/lib.rs:19:5] store.into_inner().to_string() = "AmazonS3(ookla-open-data)"
