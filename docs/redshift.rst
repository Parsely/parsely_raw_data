Redshift
==========

Loading data from s3 into Redshift can be accomplished in two steps:

1) Create a Redshift table using ``python -m parsely_raw_data.redshift create_table <args>``

2) Copy data from your Parse.ly S3 bucket using ``python -m parsely_raw_data.redshift copy_from_s3 <args>``

optional: Inspect database errors with ``python -m parsely_raw_data.redshift inspect_errors``



JSON extra_data and Redshift
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``extra_data``, where we store any customer-originated fields sent on a pixel, is a 
JSON field. Redshift does not have a native JSON data type. Instead, you have the 
option to ignore JSON fields, or load them as a JSON-formatted VARCHAR string.

Use the ``keep_extra_data`` option when creating your Redshift table if you want
to create a VARCHAR column to store `extra_data` as a JSON string:

``python -m parsely_raw_data.redshift create_table --keep_extra_data <redshift args>``

You can query JSON string-formatted fields in Redshift using their JSON-parsing functions, e.g.:

``psql>>> select json_extract_path_text(extra_data, 'subscriberType') from <tablename> where extra_data IS NOT null;``

