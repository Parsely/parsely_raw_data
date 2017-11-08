{{
    config(
        materialized='ephemeral'
    )
}}

select
    *
from {{ref('parsely_base_events')}}
where action in ('videostart','vheartbeat')
UNION all
select
  *
from {{ref('parsely_vhbs_no_vs')}}
where videostart_key in
(select distinct videostart_key from {{ref('parsely_base_events')}})
