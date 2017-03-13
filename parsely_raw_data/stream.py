from __future__ import absolute_import, print_function

import json
import threading
import time
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError, ParamValidationError
from six import iteritems
from six.moves.queue import Queue, Empty

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

"""
Consider the below a demostration of how Kinesis can be utilized with
the Parse.ly data pipeline; additional configuration may be necessary. 
Larger publishers in particular will likely need multiple processes.
"""

def events_kinesis(network, access_key_id="", secret_access_key=""):
    """Yield a stream of events from a Parse.ly Kinesis Stream

    :param network: The Parse.ly network name for which to perform reads (eg
        "blog.parsely.com")
    :type network: str
    :param access_key_id: The AWS access key to use when consuming the stream
    :type access_key_id: str
    :param secret_access_key: The AWS secret key to use when consuming the stream
    :type secret_access_key: str
    """
    client = boto3.client(
        'kinesis',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key
    )
    stream = "parsely-dw-{}".format(utils.clean_network(network))
    event_queue = Queue()

    def get_events(shard_id):
        response = client.get_shard_iterator(
            StreamName=stream,
            ShardId=shard_id,
            ShardIteratorType='LATEST'
        )
        iterator = response.get("ShardIterator", "")
        while True:
            response = {}
            try:
                response = client.get_records(ShardIterator=iterator)
            except (ClientError, ParamValidationError):
                time.sleep(2)
                continue
            iterator = response.get("NextShardIterator", "")
            records = response.get("Records", [])
            for record in records:
                event_data = record.get("Data")
                if event_data is not None:
                    try:
                        event_data = json.loads(event_data)
                    except ValueError:
                        continue
                    event_queue.put(event_data)

    workers = []
    description = {"HasMoreShards": True}
    while description.get("HasMoreShards", False):
        response = client.describe_stream(StreamName=stream)
        description = response.get('StreamDescription', {})
        shards = description.get('Shards', [])
        for shard in shards:
            worker = threading.Thread(target=get_events, args=(shard.get("ShardId"),))
            worker.daemon = True
            worker.start()
            workers.append(worker)

    while True:
        event = None
        try:
            event = event_queue.get(block=False, timeout=.01)
        except Empty:
            pass
        if event is not None:
            yield event


def main():
    parser = utils.get_default_parser(
        "Amazon Kinesis Stream utilities for Parse.ly")
    args = parser.parse_args()

    # simple example of realtime analytics with streaming event data
    # periodically prints the top ten urls in current TIME_WINDOW_SEC window
    TIME_WINDOW_SEC = 30
    url_counts = defaultdict(int)
    total_count = 0
    last_update = time.time()
    for event in events_kinesis(
            args.network,
            access_key_id=args.aws_access_key_id,
            secret_access_key=args.aws_secret_access_key):
        total_count += 1
        if event['action'] == "pageview":
            url_counts[event['url']] += 1

        if total_count % 20 == 0:
            sorted_pairs = sorted(iteritems(url_counts), key=lambda x: x[1],
                                  reverse=True)
            top_urls = sorted_pairs[:10]
            for url, events in top_urls:
                url_display = url[:70]
                if len(url) > 70:
                    url_display += "..."
                print(events, url_display)
            print("\n\n")

        if time.time() - last_update > TIME_WINDOW_SEC:
            url_counts = defaultdict(int)
            last_update = time.time()

if __name__ == "__main__":
    main()
