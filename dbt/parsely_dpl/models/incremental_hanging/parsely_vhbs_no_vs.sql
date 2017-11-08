{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='event_id'
    )
}}

select
    *
from {{ref('parsely_base_events')}}
where action = 'vheartbeat'
and  videostart_key not in
(select distinct videostart_key from {{ref('parsely_parent_videostart_keys')}})
