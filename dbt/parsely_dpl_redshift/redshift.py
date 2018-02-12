from __future__ import absolute_import

import datetime as dt
import argparse
import subprocess
import os

import parsely_raw_data.redshift as redshift
import parsely_raw_data.utils as utils

def parse_datetime_arg(arg):
    return dt.datetime.strptime(arg, '%Y%m%d')

def daterange(d1, d2):
    return (d1 + datetime.timedelta(days=i) for i in range((d2 - d1).days + 1))

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
                debut=False):

    redshift.create_table(
        table_name=table_name,
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        keep_extra_data=keep_extra_data)

    incremental(
        network=args.network,
        s3_prefix=prefix,
        table_name=args.target_table,
        host=args.redshift_host,
        user=args.redshift_user,
        password=args.redshift_password,
        database=args.redshift_database,
        port=args.redshift_port,
        keep_extra_data=args.keep_extra_data,
        access_key_id=args.aws_access_key_id,
        secret_access_key=args.aws_secret_access_key,
        start_date=start_date,
        dbt_profile_dir=args.dbt_profile_dir
    )

def incremental():
    for d in daterange(startdate, today):
        prefix = 'events/'+ d.strftime('%Y/%m/%d')
        redshift.copy_from_s3(  network=args.network,
                                s3_prefix=prefix,
                                table_name=args.target_table,
                                host=args.redshift_host,
                                user=args.redshift_user,
                                password=args.redshift_password,
                                database=args.redshift_database,
                                port=args.redshift_port,
                                access_key_id=args.aws_access_key_id,
                                secret_access_key=args.aws_secret_access_key)
        dpl_wd = os.path.join(os.getcwd(), 'dbt/parsely_dpl/')
        subprocess.call(dpl_wd + "run_parsely_dpl.sh", shell=True, cwd=dpl_wd)

def main():
    commands = ['historical','incremental']
    parser = redshift.get_default_parser("Amazon Redshift utilities for Parse.ly",
                        commands=commands)
    parser = argparse.ArgumentParser(description='Run the historical back population of Parse.ly DPL data')
    parser.add_argument('--start_date',
                        help='The first day to process data from S3 to Redshift in the format YYYYMMDD')
    parse.add_argument('--dbt_profiles_dir',
                        help='The location from root that contains the .dbt/profiles.yml file, example: /home/user/.dbt/')
    args = parser.parse_args()

#   date fields
    now = datetime.datetime.now()
    today = now.date()
    startdate = parse_datetime_arg(args.start_date).date()

#   run type
    if args.command == 'historical':
        historical(
            network=args.network,
            s3_prefix=prefix,
            table_name=args.target_table,
            host=args.redshift_host,
            user=args.redshift_user,
            password=args.redshift_password,
            database=args.redshift_database,
            port=args.redshift_port,
            keep_extra_data=args.keep_extra_data,
            access_key_id=args.aws_access_key_id,
            secret_access_key=args.aws_secret_access_key,
            start_date=startdate,
            dbt_profile_dir=args.dbt_profile_dir
            )

    elif args.command == 'incremental':
        incremental(
            network=args.network,
            s3_prefix=prefix,
            table_name=args.target_table,
            host=args.redshift_host,
            user=args.redshift_user,
            password=args.redshift_password,
            database=args.redshift_database,
            port=args.redshift_port,
            keep_extra_data=args.keep_extra_data,
            access_key_id=args.aws_access_key_id,
            secret_access_key=args.aws_secret_access_key,
            start_date=startdate,
            dbt_profile_dir=args.dbt_profile_dir
            )


if __name__ == "__main__":
    main()
