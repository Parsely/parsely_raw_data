import json
import os
import tempfile

import tablib
import xlsxwriter

from .schema import SCHEMA

def gen_json2csv(jsonlines):
    first_columns = ["action", "ts_action", "visitor_site_id", "url", "apikey"]
    headers = [field["key"] for field in SCHEMA
               if field["key"] not in first_columns]
    headers = first_columns + headers
    yield headers
    for jsonline in jsonlines:
        row = []
        for field in headers:
            cell = jsonline.get(field)
            if cell is None:
                cell = ""
            if isinstance(cell, (dict, list)):
                cell = json.dumps(cell)
            row.append(cell)
        yield row


def make_sample_dataset(jsonlines, row_limit=50000):
    lines = []
    # take up to row limit
    for i, line in enumerate(jsonlines):
        if i > row_limit:
            break
        jsonline = json.loads(line)
        lines.append(jsonline)
    headers = None
    body = []
    for i, line in enumerate(gen_json2csv(lines)):
        if i == 0:
            headers = line
            continue
        else:
            body.append(line)
    data = tablib.Dataset(*body, headers=headers)
    return data


def make_csv(rows):
    return rows.csv


def make_xlsx(rows):
    tmp_path = tempfile.mktemp()
    workbook = xlsxwriter.Workbook(tmp_path, {'strings_to_numbers': False,
                                              'strings_to_formulas': False,
                                              'strings_to_urls': False})
    worksheet = workbook.add_worksheet()
    worksheet.write_row(0, 0, rows.headers)
    for i, row in enumerate(rows):
        worksheet.write_row(i + 1, 0, row)
    workbook.close()
    contents = open(tmp_path, "rb").read()
    os.remove(tmp_path)
    return contents


def main():
    import argparse
    import gzip

    parser = argparse.ArgumentParser(
        description="Utilities to make explorable data samples from Parse.ly raw data")
    parser.add_argument("--json_input", type=str, required=True,
                        help="The path to JSONLine data (.json.gz or flat .json)")
    parser.add_argument("--output", type=str, required=True,
                        help="The path of where to write data sample (.csv or .xlsx)")
    parser.add_argument("--row_limit", type=int, default=50000,
                        help="The maximum number of rows to output")
    args = parser.parse_args()

    json_input = args.json_input
    output = args.output
    row_limit = args.row_limit
    if output.endswith(".csv"):
        open_spec = (output, "w")
        converter = make_csv
    elif output.endswith(".xlsx"):
        open_spec = (output, "wb")
        converter = make_xlsx
    else:
        print("Your --output must end with '.csv' or '.xlsx'")
        return
    with open(*open_spec) as out_file:
        if json_input.endswith(".gz"):
            jsonlines = gzip.GzipFile(json_input)
        else:
            jsonlines = open(json_input)
        data = make_sample_dataset(jsonlines, row_limit=row_limit)
        out_file.write(converter(data))


if __name__ == "__main__":
    main()


def _selftest():
    """TODO: this self-test was written to build the module, but should now
    be moved to a unit test."""
    import csv
    print("CSV file self-test...")
    tmp_path = tempfile.mktemp()
    with open(tmp_path, "w") as csv_file:
        csv_writer = csv.writer(csv_file)
        for line in gen_json2csv([{"visitor_site_id": "1",
                                   "apikey": "qz.com",
                                   "ts_action": "2016-11-26 08:30:57",
                                   "action": "pageview",
                                   "extra_data": {"present": True},
                                   "url": "http://qz.com/1234"}]):
            csv_writer.writerow(line)
    for i, line in enumerate(open(tmp_path).readlines()):
        print(line[:80])
    os.remove(tmp_path)
    print("Done.")
