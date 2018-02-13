from __future__ import absolute_import

import datetime as dt
import argparse
import subprocess
import os

import parsely_raw_data.redshift as prd_redshift
import parsely_raw_data.utils as utils

def parse_datetime_arg(arg):
    return dt.datetime.strptime(arg, '%Y%m%d')

def daterange(d1, d2):
    return (d1 + dt.timedelta(days=i) for i in range((d2 - d1).days + 1))

def historical( network="",
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
                dbt_profiles_dir="",
                debug=False):

    prd_redshift.create_table(
        table_name=table_name,
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        keep_extra_data=keep_extra_data)

    incremental(
        network=network,
        s3_prefix=s3_prefix,
        table_name=table_name,
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        keep_extra_data=keep_extra_data,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        start_date=start_date,
        dbt_profiles_dir=dbt_profiles_dir
    )

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
                dbt_profiles_dir="",
                debug=False):
    now = dt.datetime.now()
    today = now.date()
    for d in daterange(start_date, today):
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
    parser.add_argument('--run_type',help='historical or incremental')
    parser.add_argument('--start_date',
                        help='The first day to process data from S3 to Redshift in the format YYYYMMDD')
    parser.add_argument('--dbt_profiles_dir',
                        help='The location from root that contains the .dbt/profiles.yml file, example: /home/user/.dbt/')
    args = parser.parse_args()

#   date fields
    now = dt.datetime.now()
    today = now.date()
    startdate = parse_datetime_arg(args.start_date).date()

#   run type
    if args.run_type == 'historical':
        historical(
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
            dbt_profiless_dir=args.dbt_profiles_dir
            )

    elif args.run_type == 'incremental':
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
            dbt_profiles_dir=args.dbt_profiles_dir
            )


if __name__ == "__main__":
    main()
