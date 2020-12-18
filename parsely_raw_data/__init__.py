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

__version__ = "2.4.1"

from . import bigquery, docgen, redshift, s3, samples, schema, stream, utils, dbt

__all__ = [
    "bigquery",
    "docgen",
    "redshift",
    "s3",
    "samples",
    "schema",
    "stream",
    "utils",
    "dbt"
]

BOOLEAN_FIELDS = {"flags_is_amp"}


def normalize_keys(input_event_dict, schema=None):
    """Conform events to public schema: correct keys and proper value types.

    @param input_event_dict: A dictionary containing Parse.ly pixel events
    @param schema:  Optional parameter containing the schema to normalize the event_dict keys against
                    IF not specified, this will default to the most recent parsely_raw_data schema
    """
    event_dict = {}
    schema = schema or schema.SCHEMA

    # fix value types
    if input_event_dict.get("metadata.share_urls") is not None and isinstance(
        input_event_dict["metadata.share_urls"], dict
    ):
        input_event_dict["metadata.share_urls"] = (
            list(input_event_dict["metadata.share_urls"].values()) or None
        )

    # replace all "."s in the key with "_"
    input_event_dict = {x.replace('.', '_'): v for x, v in input_event_dict.items()}

    # emit only public schema items
    # ensure all columns are available and null when needed
    # account for all boolean schema defined fields as this is parsely_raw_data specific
    for key in schema:
        if key not in input_event_dict:
            if key in BOOLEAN_FIELDS:
                event_dict[key] = False
            else:
                event_dict[key] = None
        else:
            event_dict[key] = input_event_dict[key]

    event_dict["schema_version"] = __version__

    return event_dict

