import yaml
from pathlib import Path

from .default import *

SETTINGS_VAR_MAPPING = [
    {'location': 'profile', 'settings': DBT_PROFILE_NAME},
    {'location': 'parsely:events', 'settings': PARSELY_RAW_DATA_TABLE},
    {'location': 'parsely:timezone', 'settings': ETL_TIME_ZONE},
    {'location': 'parsely:actions', 'settings': ETL_PARSELY_ACTIONS},
    {'location': 'etl:keep_rawdata', 'settings': ETL_KEEP_RAW_DATA},
    {'location': 'custom:extradata', 'settings': ETL_CUSTOM_EXTRA_DATA},
    {'location': 'custom:extradataname', 'settings': ETL_CUSTOM_EXTRA_DATA_NAME},
    {'location': 'custom:actions', 'settings': ETL_CUSTOM_ACTIONS},
    {'location': 'custom:loyaltyuser', 'settings': ETL_CUSTOM_LIFETIME_LOYALITY_USER},
    {'location': 'custom:rollingloyaltyuser', 'settings': ETL_CUSTOM_ROLLING_LOYALTY_USER},
    {'location': 'custom:skimtime', 'settings': ETL_READ_SKIM_TIME_SECONDS},
    {'location': 'custom:deepreadtime', 'settings': ETL_DEEP_READ_TIME_SECONDS},
    {'location': 'custom:videoskimtime', 'settings': ETL_VIDEO_SKIM_TIME_SECONDS},
    {'location': 'custom:videodeepwatchtime', 'settings': ETL_VIDEO_DEEP_WATCH_TIME_SECONDS},
]


def migrate_settings():
    filepath = Path(__file__).parent[0] / "dbt_project.yml"

    with open(filepath) as file:
        dbt_profile = yaml.load(file, Loader=yaml.FullLoader)

    for row in SETTINGS_VAR_MAPPING:
        if row['settings']:
            if row['location'] == 'profile':
                dbt_profile[row['location']] = str(row['settings'])
            else:
                dbt_profile['vars'][row['location']] = str(row['settings'])
        continue

    with open(filepath, 'w') as file:
        yaml.dump(dbt_profile, file, default_style='"')
        stored_successfully = True

    return stored_successfully or False
