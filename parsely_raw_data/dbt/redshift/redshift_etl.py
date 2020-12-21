from __future__ import absolute_import
import logging
import pkg_resources
import psycopg2
import subprocess
from dateutil import rrule

from parsely_raw_data import redshift as parsely_redshift
from parsely_raw_data import utils as parsely_utils
from parsely_raw_data.dbt.redshift.settings import (
    DBT_PROFILE_LOCATION,
    DBT_PROFILE_TARGET_NAME,
    ETL_END_DATE,
    ETL_KEEP_RAW_DATA,
    ETL_START_DATE,
    PARSELY_RAW_DATA_TABLE,
    REDSHIFT_DATABASE,
    REDSHIFT_HOST,
    REDSHIFT_PASSWORD,
    REDSHIFT_PORT,
    REDSHIFT_USER,
    S3_AWS_ACCESS_KEY_ID,
    S3_AWS_SECRET_ACCESS_KEY,
    S3_NETWORK_NAME,
)
from parsely_raw_data.dbt.redshift.settings.merge_settings_yaml import migrate_settings

SETTINGS_ARG_MAPPING = {
    'table_name': PARSELY_RAW_DATA_TABLE,
    'host': REDSHIFT_HOST,
    'user': REDSHIFT_USER,
    'password': REDSHIFT_PASSWORD,
    'database': REDSHIFT_DATABASE,
    'port': REDSHIFT_PORT,
    'keep_extra_data': ETL_KEEP_RAW_DATA,
    'network': S3_NETWORK_NAME,
    'access_key_id': S3_AWS_ACCESS_KEY_ID,
    'secret_access_key': S3_AWS_SECRET_ACCESS_KEY,
    'start_date': ETL_START_DATE,
    'end_date': ETL_END_DATE,
    'dbt_profiles_dir': DBT_PROFILE_LOCATION,
    'dbt_target': DBT_PROFILE_TARGET_NAME
}


def get_settings_arg_mapping_value(field_name, arg_value):
    return arg_value or SETTINGS_ARG_MAPPING[field_name]


def migrate_from_s3_by_day(network=S3_NETWORK_NAME,
                table_name=PARSELY_RAW_DATA_TABLE,
                host=REDSHIFT_HOST,
                user=REDSHIFT_USER,
                password=REDSHIFT_PASSWORD,
                database=REDSHIFT_DATABASE,
                port=REDSHIFT_PORT,
                access_key_id=S3_AWS_ACCESS_KEY_ID,
                secret_access_key=S3_AWS_SECRET_ACCESS_KEY,
                start_date=ETL_START_DATE,
                end_date=ETL_END_DATE,
                dbt_profiles_dir=DBT_PROFILE_LOCATION,
                dbt_target=DBT_PROFILE_TARGET_NAME):
    """Copies data from S3 to Redshift, split into daily tasks.
    Once the daily copies are run, the full DBT ETL is executed.
    start_date and end_date are both inclusive. To run for a single day,
    the start_date and end_date should be equivalent."""
    start_date = parsely_utils.parse_datetime_arg(start_date).date()
    end_date = parsely_utils.parse_datetime_arg(end_date).date()

    # This runs the copy_from_s3 for every day between the start_date and end_date
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

    # This runs dbt once all of the new data has been copied into the raw data table
    dbt_etl_script_loc = pkg_resources.resource_filename("parsely_raw_data", "dbt/redshift/run_parsely_dpl.sh")
    dbt_etl_cwd = pkg_resources.resource_filename("parsely_raw_data", "dbt/redshift/")
    logging.info(f'Running the dbt script located at: {dbt_etl_script_loc}')
    subprocess.call(dbt_etl_script_loc + ' ' + dbt_profiles_dir + ' ' + dbt_target, shell=True, cwd=dbt_etl_cwd)


def main():
    parser = parsely_redshift.get_default_parser("Amazon Redshift utilities for Parse.ly")
    parser.add_argument('--start_date', required=False, default=ETL_START_DATE,
                        help='The first day to process data from S3 to Redshift in the format YYYY-MM-DD')
    parser.add_argument('--end_date', required=False, default=ETL_END_DATE,
                        help='The last day to process data from S3 to Redshift in the format YYYY-MM-DD')
    parser.add_argument('--dbt_profiles_dir', required=False, default=DBT_PROFILE_LOCATION,
                        help='The location from root that contains the .dbt/profiles.yml file, example: /home/user/.dbt/')
    parser.add_argument('--dbt_profile', required=False, default='parsely_dwh',
                        help='The name of the dbt profile located in the local /.dbt/profiles.yml file')
    parser.add_argument('--dbt_target', required=False, default=DBT_PROFILE_TARGET_NAME,
                        help='The target ie. dev, prod, or test to use within the dbt profiles.yml file.')
    parser.add_argument('--create-table', action='store_true', default=True,
                        help='Optional: create the Redshift Parse.ly rawdata table because it does not yet exist.')
    args = parser.parse_args()

    # Reset dbt_profile to any updated settings:
    settings_migration = migrate_settings(profile=args.dbt_profile, table=args.table_name)
    if not settings_migration:
        logging.warning("Settings not copied to dbt_profiles.yml successfully.")
        raise Exception("Settings not copied to dbt_profiles.yml successfully. Please edit default.py or copy the"
                        "original default.py.schema as default.py and edit carefully. Be mindful of single quotes"
                        "and double quotes.")

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
        except psycopg2.Error:
            logging.info(f'Table {get_settings_arg_mapping_value("table_name", args.table_name)} already exists, '
                         f'skipping create table statement.')

    migrate_from_s3_by_day(
        network=get_settings_arg_mapping_value('network', args.network),
        table_name=get_settings_arg_mapping_value('table_name', args.table_name),
        host=get_settings_arg_mapping_value('host', args.redshift_host),
        user=get_settings_arg_mapping_value('user', args.redshift_user),
        password=get_settings_arg_mapping_value('password', args.redshift_password),
        database=get_settings_arg_mapping_value('database', args.redshift_database),
        port=get_settings_arg_mapping_value('port', args.redshift_port),
        access_key_id=get_settings_arg_mapping_value('access_key_id', args.aws_access_key_id),
        secret_access_key=get_settings_arg_mapping_value('secret_access_key', args.aws_secret_access_key),
        start_date=get_settings_arg_mapping_value('start_date', args.start_date),
        end_date=get_settings_arg_mapping_value('end_date', args.end_date),
        dbt_profiles_dir=get_settings_arg_mapping_value('dbt_profiles_dir', args.dbt_profiles_dir),
        dbt_target=get_settings_arg_mapping_value('dbt_target', args.dbt_target),
        )


if __name__ == "__main__":
    main()
