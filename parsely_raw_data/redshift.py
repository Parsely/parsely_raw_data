from __future__ import absolute_import, print_function

from .schema import mk_redshift_schema

import psycopg2

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
def get_default_parser(description, commands=None):
    """Build an ArgumentParser that accepts common command line arguments
    :param description: The description for this ArgumentParser
    :type description: str
    :param commands: A sequence of valid command names
    :type commands: iterable
    """
    parser = utils.get_default_parser("Amazon Redshift utilities for Parse.ly",
                                      commands=commands)
    parser.add_argument("--table_name", type=str,
                        help="The name of the Redshift table to create/copy")
    parser.add_argument('--redshift_host', type=str,
                        help='The host string of the Redshift instance to which to '
                             'connect, ending in "redshift.amazonaws.com"')
    parser.add_argument('--redshift_user', type=str,
                        help='The user name to use when connecting to the Redshift'
                             ' instance')
    parser.add_argument('--redshift_password', type=str,
                        help='The password to use when connecting to the Redshift'
                             ' instance')
    parser.add_argument('--redshift_database', type=str,
                        help='The Redshift database to which to connect')
    parser.add_argument('--redshift_port', type=str,
                        help='The port on which to connect to Redshift')
    parser.add_argument('--keep-extra-data', action="store_true",
                        help='Optional: create a VARCHAR column for extra_data, which'
                             ' will be saved as a JSON-formatted string.')
    if commands is not None:
        parser.add_argument('command', type=str,
                            help='The operation to perform',
                            choices=commands)
    return parser

def create_table(table_name="rawdata",
                 keep_extra_data=False,
                 host="",
                 user="",
                 password="",
                 database="parsely",
                 port="5439",
                 debug=False):
    """Create a Redshift table using a schema compatible with Parse.ly events
    :param table_name: The Redshift table name to use for creation
    :type host: str
    :param host: The Redshift host on which to create the table
    :type host: str
    :param user: The Redshift user to use when creating the table
    :type user: str
    :param password: The Redshift password to use when creating the table
    :type password: str
    :param database: The name of the Redshift database on which to create the table
    :type database: str
    :param port: The port on which to connect to Redshift
    :type port: str
    """
    query = mk_redshift_schema(keep_extra_data=keep_extra_data)
    query = query.replace(u"parsely.rawdata", table_name)
    if debug:
        print("Running the following Redshift CREATE TABLE command:")
        print(query)
    connection = psycopg2.connect(host=host, port=port, user=user,
                                  password=password, database=database)
    connection.cursor().execute(query)
    connection.commit()


def copy_from_s3(network,
                 s3_prefix,
                 table_name="rawdata",
                 host="",
                 user="",
                 password="",
                 database="parsely",
                 port="5439",
                 access_key_id="",
                 secret_access_key="",
                 debug=False):
    """Use the Redshift COPY command to copy event data from S3
    :param network: The Parse.ly network for which to copy data (eg
        "parsely-blog")
    :type network: str
    :param s3_prefix: The S3 timestamp directory prefix from which to fetch data
        batches, formatted as YYYY/MM/DD
    :type s3_prefix: str
    :param table_name: The Redshift table name to use for copying
    :type host: str
    :param host: The Redshift host to which to write
    :type host: str
    :param user: The Redshift user to use when writing
    :type user: str
    :param password: The Redshift password to use when writing
    :type password: str
    :param database: The name of the Redshift database to which to write
    :type database: str
    :param port: The port on which to connect to Redshift
    :type port: str
    :param access_key_id: The AWS access key to use when copying
    :type access_key_id: str
    :param secret_access_key: The AWS secret key to use when copying
    :type secret_access_key: str
    """
    query = (
    "COPY {table_name}\n"
    "FROM 's3://parsely-dw-{network}/{s3_prefix}'\n"
    "CREDENTIALS 'aws_access_key_id={aws_access_key_id};"
    "aws_secret_access_key={aws_secret_access_key}'\n"
    "JSON AS 'auto' GZIP\n"
    "TRUNCATECOLUMNS;\n"
	).format(
	table_name=table_name,
        network=utils.clean_network(network),
        s3_prefix=s3_prefix,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key)
    if debug:
        print("Running the following Redshift COPY command:")
        print(query)
    connection = psycopg2.connect(host=host, port=port, user=user,
                                  password=password, database=database)
    connection.cursor().execute(query)
    connection.commit()


def inspect_errors(host="",
                 user="",
                 password="",
                 database="parsely",
                 port="5439"):
    connection = psycopg2.connect(host=host, port=port, user=user,
                                  password=password, database=database)
    query = """
        select le.err_reason, le.colname, le.col_length, le.raw_field_value, d.query, d.line_number
        from stl_load_errors le, stl_loaderror_detail d
        where d.query = le.query
        order by le.starttime desc
        limit 30;
        """
    cur = connection.cursor()
    cur.execute(query)
    print("error reason | colname | col length | raw field value | query | line number")
    for rec in cur:
        line = [str(piece).strip() for piece in rec]
        print(" | ".join(line))
    connection.close()



def main():
    commands = ['copy_from_s3', 'create_table', 'inspect_errors']
    parser = get_default_parser("Amazon Redshift utilities for Parse.ly",
                                commands=commands)
    args = parser.parse_args()

    if args.command == "copy_from_s3":
        copy_from_s3(
            args.network,
            args.s3_prefix,
            table_name=args.table_name,
            host=args.redshift_host,
            user=args.redshift_user,
            password=args.redshift_password,
            database=args.redshift_database,
            access_key_id=args.aws_access_key_id,
            secret_access_key=args.aws_secret_access_key,
            debug=args.debug
        )
    elif args.command == "create_table":
        create_table(
            table_name=args.table_name,
            keep_extra_data=args.keep_extra_data,
            host=args.redshift_host,
            user=args.redshift_user,
            password=args.redshift_password,
            database=args.redshift_database,
            port=args.redshift_port,
            debug=args.debug
        )
    elif args.command == "inspect_errors":
        inspect_errors(
            host=args.redshift_host,
            user=args.redshift_user,
            password=args.redshift_password,
            database=args.redshift_database,
            port=args.redshift_port
        )

if __name__ == "__main__":
    main()
