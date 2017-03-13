from __future__ import print_function

import json

from tabulate import tabulate

"""
Data Pipeline event schema DSL has this form:

    {"key": "action",     # key or column
     "ex": "pageview",    # example value
     "type": str,         # 'abstract' type
     "size": 256,         # rough size/length of field
     "req": True}         # is the field required?

Those are listed below, and then a number of functions
export these to example record formats, BigQuery/Redshift DDLs,
and documentation in Markdown format.
"""
SCHEMA = [
    {"key": "action", "ex": "pageview", "type": str, "size": 256, "req": True},
    {"key": "apikey", "ex": "mashable.com", "type": str, "size": 256, "req": True},
    {"key": "campaign_id", "ex": "facebook_campaign", "type": str, "size": 256},
    {"key": "display", "ex": True, "type": bool},
    {"key": "display_avail_height", "ex": 735, "type": int},
    {"key": "display_avail_width", "ex": 1280, "type": int},
    {"key": "display_pixel_depth", "ex": 24, "type": int},
    {"key": "display_total_height", "ex": 800, "type": int},
    {"key": "display_total_width", "ex": 1280, "type": int},
    {"key": "engaged_time_inc", "ex": None, "type": int},
    {"key": "extra_data", "ex": None, "type": object},
    {"key": "flags_is_amp", "ex": None, "type": bool},
    {"key": "ip_city", "ex": "Newark", "type": str},
    {"key": "ip_continent", "ex": "NA", "type": str, "size": 2},
    {"key": "ip_country", "ex": "US", "type": str, "size": 2},
    {"key": "ip_lat", "ex": 37.5147, "type": float},
    {"key": "ip_lon", "ex": -122.0423, "type": float},
    {"key": "ip_postal", "ex": "94560", "type": str, "size": 64},
    {"key": "ip_subdivision", "ex": "CA", "type": str, "size": 3},
    {"key": "ip_timezone", "ex": "America/Los_Angeles", "type": str, "size": 256},
    {"key": "ip_market_name", "ex": "New York", "type": str, "size": 256},
    {"key": "ip_market_nielsen", "ex": "501", "type": str, "size": 3},
    {"key": "ip_market_doubleclick", "ex": "3", "type": str, "size": 3},
    {"key": "metadata", "ex": True, "type": bool},
    {"key": "metadata_authors", "ex": ["Laura Vitto"], "type": list},
    {"key": "metadata_canonical_url", "ex": "http://mashable.com/2016/09/07/airpods-jokes/", "type": str},
    {"key": "metadata_custom_metadata", "ex": "{\"site\":\"Mashable\"}", "type": str},
    {"key": "metadata_duration", "ex": None, "type": int},
    {"key": "metadata_data_source", "ex": 'crawl', "type": str, "size": 8},
    {"key": "metadata_full_content_word_count", "ex": 174, "type": int},
    {"key": "metadata_image_url", "ex": "http://a.amz.mshcdn.com/media/ZgkyMDE2LzA5LzA3LzU2L0NyeFhpNjNYRUFBSnZwRS5lNDAyMy5qcGcKcAl0aHVtYgkxMjAweDYzMAplCWpwZw/156d0173/3ae/CrxXi63XEAAJvpE.jpg", "type": str},
    {"key": "metadata_page_type", "ex": "post", "type": str, "size": 256},
    {"key": "metadata_post_id", "ex": "http://mashable.com/2016/09/07/airpods-jokes/", "type": str},
    {"key": "metadata_pub_date_tmsp", "ex": 1473275118000, "type": int, "date": True},
    {"key": "metadata_save_date_tmsp", "ex": 1473275204000, "type": int, "date": True},
    {"key": "metadata_section", "ex": "watercooler", "type": str, "size": 256},
    {"key": "metadata_share_urls", "ex": None, "type": list},
    {"key": "metadata_tags", "ex": ["gadgets", "iphone-7"], "type": list},
    {"key": "metadata_thumb_url", "ex": "https://images.parsely.com/xY9xNBMulGDKRMzfKaUQzs7A9PA=/160x160/smart/http%3A//a.amz.mshcdn.com/media/ZgkyMDE2LzA5LzA3LzU2L0NyeFhpNjNYRUFBSnZwRS5lNDAyMy5qcGcKcAl0aHVtYgkxMjAweDYzMAplCWpwZw/156d0173/3ae/CrxXi63XEAAJvpE.jpg", "type": str},
    {"key": "metadata_title", "ex": "Everyone has the same fear about Apple's new earbuds", "type": str},
    {"key": "metadata_urls", "ex": ["http://mashable.com/2016/09/07/airpods-jokes/"], "type": list},
    {"key": "ref_category", "ex": "internal", "type": str, "size": 64},
    {"key": "ref_clean", "ex": "http://mashable.com/", "type": str},
    {"key": "ref_domain", "ex": "mashable.com", "type": str, "size": 256},
    {"key": "ref_fragment", "ex": "", "type": str},
    {"key": "ref_netloc", "ex": "mashable.com", "type": str, "size": 256},
    {"key": "ref_params", "ex": "", "type": str},
    {"key": "ref_path", "ex": "/", "type": str},
    {"key": "ref_query", "ex": "", "type": str},
    {"key": "ref_scheme", "ex": "http", "type": str, "size": 64},
    {"key": "referrer", "ex": "http://mashable.com/", "type": str},
    {"key": "session", "ex": True, "type": bool},
    {"key": "session_id", "ex": 6, "type": int},
    {"key": "session_initial_referrer", "ex": "http://mashable.com/", "type": str},
    {"key": "session_initial_url", "ex": "http://mashable.com/", "type": str},
    {"key": "session_last_session_timestamp", "ex": 1473271351611, "type": int, "date": True},
    {"key": "session_timestamp", "ex": 1473277747806, "type": int, "date": True},
    {"key": "slot", "ex": False, "type": bool},
    {"key": "sref_category", "ex": "internal", "type": str, "size": 64},
    {"key": "sref_clean", "ex": "http://mashable.com/", "type": str},
    {"key": "sref_domain", "ex": "mashable.com", "type": str, "size": 256},
    {"key": "sref_fragment", "ex": "", "type": str},
    {"key": "sref_netloc", "ex": "mashable.com", "type": str, "size": 256},
    {"key": "sref_params", "ex": "", "type": str},
    {"key": "sref_path", "ex": "/", "type": str},
    {"key": "sref_query", "ex": "", "type": str},
    {"key": "sref_scheme", "ex": "http", "type": str, "size": 64},
    {"key": "surl_clean", "ex": "http://mashable.com/", "type": str},
    {"key": "surl_domain", "ex": "mashable.com", "type": str, "size": 256},
    {"key": "surl_fragment", "ex": "", "type": str},
    {"key": "surl_netloc", "ex": "mashable.com", "type": str, "size": 256},
    {"key": "surl_params", "ex": "", "type": str},
    {"key": "surl_path", "ex": "/", "type": str},
    {"key": "surl_query", "ex": "", "type": str},
    {"key": "surl_scheme", "ex": "http", "type": str, "size": 64},
    {"key": "timestamp_info", "ex": True, "type": bool},
    {"key": "timestamp_info_nginx_ms", "ex": 1473277850000, "type": int, "date": True, "req": True},
    {"key": "timestamp_info_override_ms", "ex": None, "type": int, "date": True},
    {"key": "timestamp_info_pixel_ms", "ex": 1473277850017, "type": int, "date": True},
    {"key": "ts_action", "ex": "2016-09-07 19:50:50", "type": str, "date": True},
    {"key": "ts_session_current", "ex": "2016-09-07 19:49:07", "type": str, "date": True},
    {"key": "ts_session_last", "ex": "2016-09-07 18:02:31", "type": str, "date": True},
    {"key": "ua_browser", "ex": "Safari", "type": str},
    {"key": "ua_browserversion", "ex": "9.1.2", "type": str},
    {"key": "ua_device", "ex": "Other", "type": str},
    {"key": "ua_devicebrand", "ex": None, "type": str},
    {"key": "ua_devicemodel", "ex": None, "type": str},
    {"key": "ua_devicetouchcapable", "ex": True, "type": bool},
    {"key": "ua_devicetype", "ex": "desktop", "type": str},
    {"key": "ua_os", "ex": "Mac OS X", "type": str},
    {"key": "ua_osversion", "ex": "10.10.5", "type": str},
    {"key": "url", "ex": "http://mashable.com/2016/09/07/airpods-jokes/#L.eZPflSGqq5", "type": str},
    {"key": "url_clean", "ex": "http://mashable.com/2016/09/07/airpods-jokes/", "type": str},
    {"key": "url_domain", "ex": "mashable.com", "type": str, "size": 256},
    {"key": "url_fragment", "ex": "L.eZPflSGqq5", "type": str},
    {"key": "url_netloc", "ex": "mashable.com", "type": str, "size": 256},
    {"key": "url_params", "ex": "", "type": str},
    {"key": "url_path", "ex": "/2016/09/07/airpods-jokes/", "type": str},
    {"key": "url_query", "ex": "", "type": str},
    {"key": "url_scheme", "ex": "http", "type": str, "size": 64},
    {"key": "utm_campaign", "ex": "facebook_campaign", "type": str, "size": 256},
    {"key": "utm_medium", "ex": "partners", "type": str, "size": 64},
    {"key": "utm_source", "ex": "facebook", "type": str, "size": 64},
    {"key": "utm_term", "ex": "8098", "type": str, "size": 64},
    {"key": "utm_content", "ex": "sports", "type": str, "size": 256},
    {"key": "user_agent", "ex": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/601.7.7 (KHTML, like Gecko) Version/9.1.2 Safari/601.7.7", "type": str},
    {"key": "version", "ex": 1, "type": int},
    {"key": "visitor", "ex": True, "type": bool},
    {"key": "visitor_ip", "ex": "108.225.131.20", "type": str, "size": 256},
    {"key": "visitor_network_id", "ex": "ac94fe31-a307-4020-9a23-fa4798217b02", "type": str, "size": 128},
    {"key": "visitor_site_id", "ex": "ab94fd31-a207-4010-8a25-fb4788207b82", "type": str, "size": 128, "req": True}
]


def mk_sample_event():
    sample = {}
    for record in SCHEMA:
        sample[record["key"]] = record["ex"]
    return sample


def mk_bigquery_table():
    table = []
    headers = ["Column", "Example", "Type"]
    for record in SCHEMA:
        key = record["key"]
        example = record["ex"]
        types2str = {
            str: "STRING",
            int: "INTEGER",
            float: "FLOAT",
            bool: "BOOLEAN",
            object: "JSON",
            list: "STRING (REPEATED)"
        }
        type_ = types2str[record["type"]]
        if type_ == "STRING":
            if example and len(example) > 25:
                example = example[0:23] + "..."
            example = repr(example)
        table.append([key, example, type_])
    return table, headers


def mk_bigquery_schema():
    table, headers = mk_bigquery_table()
    jsonlines = []
    for row in table:
        key, _, type_ = row
        if type_ == "JSON":
            # skip json types since it requires additional
            # modelling from the user
            continue
        if "REPEATED" in type_:
            type_ = type_.split(" ")[0]
            mode = "REPEATED"
        else:
            mode = "NULLABLE"
        jsonlines.append({
            "name": key,
            "type": type_,
            "mode": mode
        })
    return jsonlines


def mk_spark_sql_schema(with_visitor_partition=False):
    from pyspark.sql.types import (
        ArrayType, BooleanType, DoubleType, IntegerType,
        LongType, StringType, StructType)

    schema = StructType()
    for field in SCHEMA:
        k = field['key']
        typ = field['type']
        if k == "metadata_share_urls":
            schema.add(k, StringType())
        elif typ is str:
            schema.add(k, StringType())
        elif typ is int:
            schema.add(k, LongType())
        elif typ is float:
            schema.add(k, DoubleType())
        elif typ is bool:
            schema.add(k, BooleanType())
        elif typ is object:
            schema.add(k, StringType())
        elif typ is list:
            schema.add(k, ArrayType(StringType()))
        else:
            raise TypeError, "Don't know how to parse field %s of type %s" % (k, typ)
    if with_visitor_partition:
        schema.add("visitor_partition", IntegerType())
    return schema


def mk_redshift_table():
    table = []
    headers = ["Column", "Example", "Type"]
    for record in SCHEMA:
        key = record["key"]
        example = record["ex"]
        types2str = {
            str: "VARCHAR(4096)",
            int: "INTEGER",
            float: "FLOAT",
            bool: "BOOLEAN",
            object: "JSON",
            list: "VARCHAR(MAX)"
        }
        type_ = types2str[record["type"]]
        if type_ == "VARCHAR(4096)":
            if record.get("size", False):
                type_ = "VARCHAR(%s)" % record["size"]
            if example and len(example) > 25:
                example = example[0:23] + "..."
            example = repr(example)
        if record.get("date", False) and key.startswith("ts_"):
            type_ = "TIMESTAMP"
        elif record.get("date", False):
            type_ = "BIGINT"
        if record.get("req", False):
            type_ = type_ + " NOT NULL"
        table.append([key, example, type_])
    return table, headers


def mk_redshift_schema():
    table, headers = mk_redshift_table()
    ddl = []
    # open
    ddl.append("CREATE TABLE parsely.rawdata (")
    for row in table:
        key, _, type_ = row
        if type_ == "JSON":
            # skip JSON type since we can't do anything with it
            # without additional ETL steps
            continue
        # each line of DDL with key/type
        ddl.append("{:8}{:35} {:25}".format(" ", key, type_ + ","))
    # strip trailing comma
    ddl[-1] = ddl[-1].replace(",", "")
    # close it up
    ddl.append(");")
    return "\n".join(ddl)


def mk_markdown_table(prefix=None):
    table = []
    headers = ["Key", "Example", "Type"]
    for record in SCHEMA:
        key = record["key"]
        if prefix is not None:
            if not key.startswith(prefix):
                continue
        example = record["ex"]
        types2str = {
            str: "STRING",
            int: "INTEGER",
            float: "FLOAT",
            bool: "BOOLEAN",
            object: "JSON",
            list: "LIST"
        }
        type_ = types2str[record["type"]]
        if type_ == "STRING":
            if example and len(example) > 25:
                example = example[0:23] + "..."
            example = repr(example)
        if record.get("date", False):
            type_ = "DATE (%s)" % type_
        table.append([key, example, type_])
    return table, headers


if __name__ == "__main__":
    # TODO: CLI for just showing the various DDLs
    from .docgen import main
    main()
