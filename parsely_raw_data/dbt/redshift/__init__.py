from __future__ import absolute_import

from .redshift_etl import migrate_from_s3_by_day
from .settings import (
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
from .settings import migrate_settings