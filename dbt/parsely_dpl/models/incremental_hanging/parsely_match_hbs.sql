{{
    config(
        materialized='ephemeral'
    )
}}

select
    *
from {{ref('parsely_base_events')}}
where action in ('pageview','heartbeat')
UNION all
select
  *
from {{ref('parsely_hbs_no_pvs')}}
where pageview_key in
(select distinct pageview_key from {{ref('parsely_base_events')}})
