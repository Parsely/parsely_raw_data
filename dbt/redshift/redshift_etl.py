from __future__ import absolute_import
import logging
import subprocess
import os

from dateutil import rrule
from sqlalchemy.exc import ProgrammingError

from parsely_raw_data import redshift as parsely_redshift
from parsely_raw_data import utils as parsely_utils
from dbt.redshift.settings.default import *
from dbt.redshift.settings.merge_settings_yaml import migrate_settings

SETTINGS_ARG_MAPPING = {
    'table_name': PARSELY_RAW_DATA_TABLE,
    'host': REDSHIFT_HOST,
    'user': REDSHIFT_USER,
    'password': REDSHIFT_PASSWORD,
    'database': REDSHIFT_DATABASE,
    'port': REDSHIFT_PORT,
    'keep_extra_data': ETL_KEEP_RAW_DATA,
}


def get_settings_arg_mapping_value(field_name, arg_value):
    if arg_value:
        return arg_value
    else:
        settings_value = SETTINGS_ARG_MAPPING[field_name]
        return settings_value


def migrate_from_s3_by_day(network=None,
                table_name=PARSELY_RAW_DATA_TABLE,
                host=REDSHIFT_HOST,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD,
                database=REDSHIFT_DATABASE,
                port=REDSHIFT_PORT,
                keep_extra_data=ETL_KEEP_RAW_DATA,
                access_key_id=S3_AWS_ACCESS_KEY_ID,
                secret_access_key=S3_AWS_SECRET_ACCESS_KEY,
                start_date=ETL_START_DATE,
                end_date=ETL_END_DATE,
                dbt_profiles_dir=DBT_PROFILE_LOCATION,
                dbt_target=DBT_PROFILE_TARGET_NAME,
                debug=False):

    for d in rrule.rrule(rrule.DAILY, interval=1, dtstart=start_date, until=end_date):
        prefix = 'events/'+ d.strftime('%Y/%m/%d')
        parsely_redshift.copy_from_s3(network=network,
                                s3_prefix=prefix,
                                table_name=table_name,
                                host=host,
                                user=user,
                                password=password,
                                database=database,
                                port=port,
                                access_key_id=access_key_id,
                                secret_access_key=secret_access_key)
        dpl_wd = os.path.join(os.getcwd(), 'dbt/redshift/')
        subprocess.call(dpl_wd + "run_parsely_dpl.sh " + dbt_profiles_dir + ' ' + dbt_target, shell=True, cwd=dpl_wd)


def main():
    parser = parsely_redshift.get_default_parser("Amazon Redshift utilities for Parse.ly")
    parser.add_argument('--start_date', required=True, default=ETL_START_DATE,
                        help='The first day to process data from S3 to Redshift in the format YYYY-MM-DD')
    parser.add_argument('--end_date', required=True, default=ETL_END_DATE,
                        help='The last day to process data from S3 to Redshift in the format YYYY-MM-DD')
    parser.add_argument('--dbt_profiles_dir', required=True, default=DBT_PROFILE_LOCATION,
                        help='The location from root that contains the .dbt/profiles.yml file, example: /home/user/.dbt/')
    parser.add_argument('--dbt_target', required=True, default=DBT_PROFILE_TARGET_NAME,
                        help='The target ie. dev, prod, or test to use within the dbt profiles.yml file.')
    parser.add_argument('--create-table', action='store_true', default=True,
                        help='Optional: create the Redshift Parse.ly rawdata table because it does not yet exist.')
    args = parser.parse_args()

    # Reset dbt_profile to any updated settings:
    settings_migration = migrate_settings()
    if not settings_migration:
        logging.warning("Settings not copied to dbt_profiles.yml succcessfully.")

    # Handle defaults
    if args.create_table:
        try:
            parsely_redshift.create_table(
                table_name=get_settings_arg_mapping_value('table_name', args.table_name),
                host=get_settings_arg_mapping_value('host', args.redshift_host),
                user=get_settings_arg_mapping_value('user', args.redshift_user),
                password=get_settings_arg_mapping_value('password', args.redshift_password),
                database=get_settings_arg_mapping_value('database', args.redshift_database),
                port=get_settings_arg_mapping_value('port', args.redshift_port),
                keep_extra_data=get_settings_arg_mapping_value('keep_extra_data', args.keep_extra_data)
            )
        except ProgrammingError:
            logging.warning("Table already exists, skipping create table statement.")

    migrate_from_s3_by_day(
        network=args.network,
        table_name=args.table_name,
        host=args.redshift_host,
        user=args.redshift_user,
        password=args.redshift_password,
        database=args.redshift_database,
        port=args.redshift_port,
        keep_extra_data=args.keep_extra_data,
        access_key_id=args.aws_access_key_id,
        secret_access_key=args.aws_secret_access_key,
        start_date=args.start_date,
        end_date=args.end_date,
        dbt_profiles_dir=args.dbt_profiles_dir,
        dbt_target=args.dbt_target
        )


if __name__ == "__main__":
    main()
