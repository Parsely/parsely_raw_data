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
where action = 'heartbeat'
and pageview_key not in
(select distinct pageview_key from {{ref('parsely_parent_pageview_keys')}})
