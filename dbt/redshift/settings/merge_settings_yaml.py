import yaml
from dbt.redshift.settings.default import *

SETTINGS_VAR_MAPPING = [
    {'location': 'profile', 'settings': DBT_PROFILE_LOCATION},
    {'location': 'vars["parsely:events"]', 'settings': PARSELY_RAW_DATA_TABLE},
    {'location': 'vars["parsely:timezone"]', 'settings': ETL_TIME_ZONE},
    {'location': 'vars["parsely:actions"]', 'settings': ETL_PARSELY_ACTIONS},
    {'location': 'vars["etl:keep_rawdata"]', 'settings': ETL_KEEP_RAW_DATA},
    {'location': 'vars["custom:extradataname"]', 'settings': ETL_CUSTOM_EXTRA_DATA_NAME},
    {'location': 'vars["custom:actions"]', 'settings': ETL_CUSTOM_ACTIONS},
    {'location': 'vars["custom:loyaltyuser"]', 'settings': ETL_CUSTOM_LIFETIME_LOYALITY_USER},
    {'location': 'vars["custom:rollingloyaltyuser"]', 'settings': ETL_CUSTOM_ROLLING_LOYALTY_USER},
    {'location': 'vars["custom:skimtime"]', 'settings': ETL_READ_SKIM_TIME_SECONDS},
    {'location': 'vars["custom:deepreadtime"]', 'settings': ETL_DEEP_READ_TIME_SECONDS},
    {'location': 'vars["custom:videoskimtime"]', 'settings': ETL_VIDEO_SKIM_TIME_SECONDS},
    {'location': 'vars["custom:videodeepwatchtime"]', 'settings': ETL_VIDEO_DEEP_WATCH_TIME_SECONDS},
]


def migrate_settings():
    with open(r'..dbt_project.yml') as file:
        dbt_profile = yaml.load(file, Loader=yaml.FullLoader)

    for row in SETTINGS_VAR_MAPPING:
        if row['settings']:
            dbt_profile[row['location']] = row['settings']
        else:
            continue

    with open(r'..dbt_project.yml', 'w') as file:
        document = yaml.dump(dbt_profile, file)

    if document:
        return True
    else:
        return False
