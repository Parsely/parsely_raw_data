from __future__ import print_function

import json

from tabulate import tabulate

from .schema import (mk_sample_event,
                     mk_markdown_table,
                     mk_redshift_table,
                     mk_redshift_schema,
                     mk_bigquery_table,
                     mk_bigquery_schema)


def jsonprint(obj):
    print(json.dumps(obj, indent=4, sort_keys=True))


def tableprint(table, headers):
    print(tabulate(table, headers, tablefmt="pipe"))


def br():
    print()


def main():
    # TODO: CLI for creating various Markdown files
    # which we can use for documentation in the repo,
    # and we'll auto-generate them on release with GNU Make?
    print("## Sample Event")
    br()
    print("```javascript")
    jsonprint(mk_sample_event())
    print("```")
    br()
    print("## Schema Overview")
    br()
    tableprint(*mk_markdown_table())
    br()
    print("## BigQuery Schema")
    br()
    tableprint(*mk_bigquery_table())
    br()
    print("## BigQuery DDL")
    br()
    print("```javascript")
    jsonprint(mk_bigquery_schema())
    print("```")
    br()
    print("## Redshift Schema")
    br()
    tableprint(*mk_redshift_table())
    br()
    print("## Redshift DDL")
    br()
    print("```sql")
    print(mk_redshift_schema())
    print("```")


if __name__ == "__main__":
    main()
