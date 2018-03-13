from __future__ import absolute_import

import datetime as dt
import argparse
import subprocess
import os

from dateutil import rrule
from parsely_raw_data import redshift as prd_redshift
from parsely_raw_data import utils as utils

def incremental(network="",
                s3_prefix="",
                table_name="rawdata",
                host="",
                user="",
                password="",
                database="parsely",
                port="5439",
                keep_extra_data=False,
                access_key_id="",
                secret_access_key="",
                start_date="",
                end_date=dt.datetime.now().date(),
                dbt_profiles_dir="",
                debug=False):

    for d in rrule.rrule(rrule.DAILY, interval=1, dtstart=start_date, until=end_date):
        prefix = 'events/'+ d.strftime('%Y/%m/%d')
        prd_redshift.copy_from_s3(  network=network,
                                s3_prefix=prefix,
                                table_name=table_name,
                                host=host,
                                user=user,
                                password=password,
                                database=database,
                                port=port,
                                access_key_id=access_key_id,
                                secret_access_key=secret_access_key)
        dpl_wd = os.path.join(os.getcwd(), 'parsely_raw_data/dbt/parsely_dpl_redshift/')
        subprocess.call(dpl_wd + "run_parsely_dpl.sh " + dbt_profiles_dir, shell=True, cwd=dpl_wd)

def main():
    parser = prd_redshift.get_default_parser("Amazon Redshift utilities for Parse.ly")
    parser.add_argument('--start_date',
                        help='The first day to process data from S3 to Redshift in the format YYYY-MM-DD')
    parser.add_argument('--end_date',
                        help='The last day to process data from S3 to Redshift in the format YYYY-MM-DD')
    parser.add_argument('--dbt_profiles_dir',
                        help='The location from root that contains the .dbt/profiles.yml file, example: /home/user/.dbt/')
    parser.add_argument('--create-table', action='store_true',
                        help='Optional: create the Redshift Parse.ly rawdata table because it does not yet exist.')
    args = parser.parse_args()

#   date fields
    startdate = utils.parse_datetime_arg(args.start_date).date()
    enddate = utils.parse_datetime_arg(args.end_date).date()

#   run type
    if args.create_table:
        prd_redshift.create_table(
            table_name=args.table_name,
            host=args.redshift_host,
            user=args.redshift_user,
            password=args.redshift_password,
            database=args.redshift_database,
            port=args.redshift_port,
            keep_extra_data=args.keep_extra_data
        )

    incremental(
        network=args.network,
        s3_prefix=args.s3_prefix,
        table_name=args.table_name,
        host=args.redshift_host,
        user=args.redshift_user,
        password=args.redshift_password,
        database=args.redshift_database,
        port=args.redshift_port,
        keep_extra_data=args.keep_extra_data,
        access_key_id=args.aws_access_key_id,
        secret_access_key=args.aws_secret_access_key,
        start_date=startdate,
        end_date=enddate,
        dbt_profiles_dir=args.dbt_profiles_dir
        )


if __name__ == "__main__":
    main()
