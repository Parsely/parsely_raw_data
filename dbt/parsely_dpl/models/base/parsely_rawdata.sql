{{
    config(
        materialized='incremental',
        sql_where = 'TRUE',
        unique_key='event_id'
    )
}}

-- created to track event_ids for duplicate event_ids that do not need to be processed twice
-- how often should this truncate?
select
  *,
  CURRENT_TIMESTAMP as insert_timestamp
from {{ var('parsely:events') }}
