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

__version__ = '2.3.0.dev0'

from . import bigquery, docgen, redshift, s3, samples, schema, stream, utils
from six import iteritems

__all__ = [
    'bigquery',
    'docgen',
    'redshift',
    's3',
    'samples',
    'schema',
    'stream',
    'utils',
]

def normalize_keys(r, schema):
    """Conform events to public schema: correct keys and proper value types."""
    schema = schema or schema.SCHEMA
    event_dict = {}
    version =__version__

    # fix value types
    if r.get("metadata.share_urls") is not None and isinstance(
        r["metadata.share_urls"], dict
    ):
        r["metadata.share_urls"] = list(r["metadata.share_urls"].values()) or None

    # emit only public schema items
    for key, val in iteritems(r):
        key = key.replace(".", "_")
        if key in schema:
            event_dict[key] = val

    # ensure all columns are available and null when needed
    for key in schema:
        if key not in event_dict.keys():
            event_dict[key] = None

    event_dict["version"] = version

    return event_dict
