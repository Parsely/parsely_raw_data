from __future__ import absolute_import, print_function

import argparse
import datetime as dt

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
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--network', type=str,
                        help='The network for which to perform the operation')
    parser.add_argument('--aws_access_key_id', type=str,
                        help='The AWS access key to use when connecting')
    parser.add_argument('--aws_secret_access_key', type=str,
                        help='The AWS secret key to use when connecting')
    parser.add_argument('--aws_region_name', type=str,
                        help='The AWS region to which to connect')
    parser.add_argument('--s3_prefix', type=str,
                        help='The date prefix to use when copying from S3, formatted as '
                             'YYYY/MM/DD')
    parser.add_argument('--debug', action='store_true',
                        help='Turn on debug mode to log output and commands')
    if commands is not None:
        parser.add_argument('command', type=str,
                            help='The operation to perform',
                            choices=commands)
    return parser


def clean_network(network):
    """Format a network name to match AWS resources"""
    return network.replace(".", "-").replace(" ", "-").lower()

def parse_datetime_arg(arg):
    return dt.datetime.strptime(arg, '%Y-%m-%d')
