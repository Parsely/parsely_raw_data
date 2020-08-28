from __future__ import absolute_import, print_function

import logging
import pprint
import json

from googleapiclient.discovery import build as google_build
from oauth2client.client import GoogleCredentials
from six import iteritems

from . import utils
from .s3 import events_s3
from .schema import mk_bigquery_schema


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

log = logging.getLogger(__name__)


def streaming_insert_bigquery(jsonlines,
                              bq_conn=None,
                              project_id=None,
                              dataset_id=None,
                              table_id=None):
    """Write a stream of events to BigQuery

    :param bq_conn: The BigQuery connection to write to
    :type bq_conn: googleapiclient.discovery.Resource
    :param project_id: The BigQuery project ID to write to
    :type project_id: str
    :param dataset_id: The BigQuery dataset ID to write to
    :type dataset_id: str
    :param table_id: The BigQuery table ID to write to
    :type table_id: str
    """
    insert_body = {
        "kind": "bigquery#tableDataInsertAllRequest",
        "skipInvalidRows": False,
        "ignoreUnknownValues": False,
        "rows": [
            {"json": line} for line in jsonlines
        ]
    }
    if bq_conn is None:
        pprint.PrettyPrinter(indent=0).pprint(insert_body)
        return False
    query = bq_conn.tabledata().insertAll(projectId=project_id,
                                          datasetId=dataset_id,
                                          tableId=table_id,
                                          body=insert_body)
    response = query.execute(num_retries=5)
    if 'insertErrors' in response:
        for error_set in response['insertErrors']:
            for error in error_set['errors']:
                log.error(error)
        return False
    return True


def copy_from_s3(network,
                 s3_prefix="",
                 access_key_id="",
                 secret_access_key="",
                 region_name="us-east-1",
                 project_id=None,
                 dataset_id=None,
                 table_id=None,
                 dry_run=False):
    """Load events from S3 to BigQuery using the BQ streaming insert API.

    :param network: The Parse.ly network for which to perform writes (eg
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
    :param project_id: The BigQuery project ID to write to
    :type project_id: str
    :param dataset_id: The BigQuery dataset ID to write to
    :type dataset_id: str
    :param table_id: The BigQuery table ID to write to
    :type table_id: str
    :param dry_run: If True, don't perform BigQuery writes
    :type dry_run: bool
    """
    bq_conn = None
    if not dry_run:
        bq_conn = google_build(
            'bigquery', 'v2',
            credentials=GoogleCredentials.get_application_default())
    s3_stream = events_s3(network, prefix=s3_prefix, access_key_id=access_key_id,
                          secret_access_key=secret_access_key,
                          region_name=region_name)

    schema_compliant_fields = [column['name'] for column in mk_bigquery_schema()]

    def schema_compliant(jsonline):
        return {k: jsonline.get(k, None) for k in schema_compliant_fields}

    def chunked(seq, chunk_size):
        chunk = []
        for item in seq:
            chunk.append(schema_compliant(item))
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    for events in chunked(s3_stream, 500):
        streaming_insert_bigquery(events, bq_conn=bq_conn, project_id=project_id,
                              dataset_id=dataset_id, table_id=table_id)



def create_table(project_id, table_id, dataset_id, debug=False):
    """Create a BigQuery table using a schema compatible with Parse.ly events

    :param project_id: The BigQuery project ID to write to
    :type project_id: str
    :param table_id: The BigQuery table ID to write to
    :type table_id: str
    :param dataset_id: The BigQuery dataset ID to write to
    :type dataset_id: str
    """
    fields = mk_bigquery_schema()
    schema = {
        "description": "Parse.ly Data Pipeline",
        "schema": {"fields": fields},
        "tableReference": {
            "projectId": project_id,
            "tableId": table_id,
            "datasetId": dataset_id
        }
    }
    if debug:
        print("Running the following BigQuery JSON table insert:")
        print(json.dumps(schema, indent=4, sort_keys=True))
    credentials = GoogleCredentials.get_application_default()
    bigquery = google_build('bigquery', 'v2', credentials=credentials)
    bigquery.tables().insert(projectId=project_id,
                             datasetId=dataset_id,
                             body=schema).execute()


def main():
    commands = ["copy_from_s3", "create_table"]
    parser = utils.get_default_parser("Google BigQuery utilities for Parse.ly",
                                      commands=commands)
    parser.add_argument('--dry_run', action="store_true",
                        help="If true, don't perform writes to Bigquery"
                             'connect, ending in "redshift.amazonaws.com"')
    parser.add_argument('--bigquery_project_id', type=str,
                        help='The ID of the BigQuery project to which to connect')
    parser.add_argument('--bigquery_dataset_id', type=str,
                        help='The ID of the BigQuery dataset to which to connect')
    parser.add_argument('--bigquery_table_id', type=str,
                        help='The ID of the BigQuery table to which to connect')
    args = parser.parse_args()

    if args.command == "copy_from_s3":
        copy_from_s3(
            args.network,
            s3_prefix=args.s3_prefix,
            access_key_id=args.aws_access_key_id,
            secret_access_key=args.aws_secret_access_key,
            region_name=args.aws_region_name,
            project_id=args.bigquery_project_id,
            dataset_id=args.bigquery_dataset_id,
            table_id=args.bigquery_table_id,
            dry_run=args.dry_run
        )
    elif args.command == "create_table":
        create_table(
            project_id=args.bigquery_project_id,
            dataset_id=args.bigquery_dataset_id,
            table_id=args.bigquery_table_id,
            debug=args.debug
        )

if __name__ == "__main__":
    main()
