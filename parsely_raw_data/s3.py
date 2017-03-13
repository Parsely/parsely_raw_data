from __future__ import absolute_import, print_function

import gzip
import json
from collections import defaultdict
from io import BytesIO

import boto3

from . import utils


__license__ = """
Copyright 2016 Parsely, Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""


def events_s3(network,
              prefix="",
              access_key_id="",
              secret_access_key="",
              region_name="us-east-1"):
    """Yield a stream of events from a Parse.ly S3 bucket

    :param network: The Parse.ly network for which to perform reads (eg
        "parsely-blog")
    :type network: str
    :param s3_prefix: The S3 timestamp directory prefix from which to fetch data
        batches, formatted as YYYY/MM/DD
    :type s3_prefix: str
    :param access_key_id: The AWS access key to use when fetching data batches
    :type access_key_id: str
    :param secret_access_key: The AWS secret key to use when fetching data batches
    :type secret_access_key: str
    :param region_name: The AWS region in which to perform fetches
    :type region_name: str
    """
    bucket = "parsely-dw-{}".format(utils.clean_network(network))
    client = boto3.client('s3', **{
        'aws_access_key_id': access_key_id,
        'aws_secret_access_key': secret_access_key,
        'region_name': region_name
    })
    res = {"IsTruncated": True}
    while res.get("IsTruncated", False):
        res = client.list_objects(Bucket=bucket, Prefix=prefix)
        for item in res.get("Contents", []):
            obj = client.get_object(Bucket=bucket, Key=item.get("Key"))
            with gzip.GzipFile(fileobj=BytesIO(obj.get("Body").read()),
                               mode="rb") as f:
                for part in [a for a in f.read().split('\n') if a]:
                    yield json.loads(part)


def main():
    args = utils.get_default_parser("Amazon S3 utilities for Parse.ly").parse_args()
    event_counts = defaultdict(int)
    for event in events_s3(
            args.network,
            prefix=args.s3_prefix,
            access_key_id=args.aws_access_key_id,
            secret_access_key=args.aws_secret_access_key):
        event_counts[event.action] += 1
    print(event_counts)


if __name__ == "__main__":
    main()
