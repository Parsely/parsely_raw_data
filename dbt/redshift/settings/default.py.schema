import datetime as dt

today = dt.datetime.today()
yesterday = today - dt.timedelta(1)

# This file contains all settings that should be set to run dbt for Parse.ly Data Pipeline using Redshift

# ***** Redshift settings *****
# The name of the table that contains the raw data inside the schema specified in ~/.dbt/profiles.yml file
PARSELY_RAW_DATA_TABLE = 'parsely_rawdata'

# The Redshift host as specified in the Redshift settings, takes the format: 'a-unique-name.domain.com'
REDSHIFT_HOST = ''

# The Redshift credentials that will be performing the create table and
# insert into table operations; Must have the correct permissions
# You may also use environment variables here, edit as needed
REDSHIFT_USER = None
REDSHIFT_PASSWORD = None

# The redshift database and port that will contain the Redshift Data Pipeline data
REDSHIFT_DATABASE = 'parsely'
REDSHIFT_PORT = '5439'

# ***** S3 settings *****
# The S3 bucket that contains the Parse.ly Data Pipeline data
S3_AWS_ACCESS_KEY_ID = None
S3_AWS_SECRET_ACCESS_KEY = None
S3_BUCKET_NAME = None
S3_NETWORK_NAME = None  # everything after the `parsely-dw` in the bucket name

# ***** DBT settings *****
DBT_PROFILE_LOCATION = '~/.dbt/'
DBT_PROFILE_NAME = 'parsely-dwh'
DBT_PROFILE_TARGET_NAME = 'dev'

# ***** ETL settings *****
# The start and end dates for the ETL. This is typically set programmatically to run daily.
ETL_START_DATE = yesterday.strftime('%Y-%m-%d')
ETL_END_DATE = today.strftime('%Y-%m-%d')

# All timestamps are in UTC. Specifiy the timezone that the data will be reported in.
# Available options here: https://docs.aws.amazon.com/redshift/latest/dg/time-zone-names.html
ETL_TIME_ZONE = 'UTC'

# Parse.ly standard events. Recommended to leave as-is: "('pageview','heartbeat','videostart','vheartbeat')"
ETL_PARSELY_ACTIONS = "('pageview','heartbeat','videostart','vheartbeat')"

# True to keep all incremental raw data in the Redshift table PARSELY_RAW_DATA_TABLE,
# False to truncate this table with every run
ETL_KEEP_RAW_DATA = True

# The key in the json dictionary passed through the extra_data field: { "key" : "value" } Example: "'userType'"
ETL_CUSTOM_EXTRA_DATA = "''"

# The desired Redshift column name of the extra data field listed above (could be equivalent) Example: 'user_type'
ETL_CUSTOM_EXTRA_DATA_NAME = 'custom_extra_data'

# A list of any custom actions sent through the Parse.ly DPL in the format:
# 'custom:actions': "('list','custom','actions','individually')"
ETL_CUSTOM_ACTIONS = "('conversion')"

# Number of lifetime pageviews from the start of Parse.ly DPL data tracking that identify a user as loyalty
ETL_CUSTOM_LIFETIME_LOYALITY_USER = '100'

# Number of rolling 30 days pageviews that identify a user as a current loyalty user
ETL_CUSTOM_ROLLING_LOYALTY_USER = '30'

## Reading and video watching categories:
# Time in seconds where reading engaged time less than this amount is considered a 'skim'
ETL_READ_SKIM_TIME_SECONDS = '15'
# Time in seconds where reading engaged time greater than this amount is considered a 'deep read'
ETL_DEEP_READ_TIME_SECONDS = '40'
# Time in seconds where video engaged time less than this amount is considered a 'skim'
ETL_VIDEO_SKIM_TIME_SECONDS = '15'
# Time in seconds where video engaged time greater than this amount is considered a 'deep watch'
ETL_VIDEO_DEEP_WATCH_TIME_SECONDS = '60'
