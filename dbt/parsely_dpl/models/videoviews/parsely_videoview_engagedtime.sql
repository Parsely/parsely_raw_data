with videostart_events as (

    select * from {{ ref('parsely_match_vhbs') }}

),

engaged_xf as (

  select
      vs.event_id,
      sum(vhb.engaged_time_inc) as engaged_time
  from videostart_events vhb
  left join {{ref('parsely_match_vhbs')}} vs using (videostart_key)
  where vhb.action = 'vheartbeat' and vs.action = 'videostart' and
  vhb.ts_action >= vs.ts_action and
  (case when vs.next_pageview_ts_action is not null
    then vhb.ts_action < vs.next_pageview_ts_action
    else true end)
  group by vs.event_id
)

select
  *
from videostart_events
where action = 'videostart'
left join engaged_xf using (event_id)
