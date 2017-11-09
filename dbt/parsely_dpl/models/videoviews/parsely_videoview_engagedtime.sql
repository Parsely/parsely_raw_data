with videostart_events as (

    select * from {{ ref('parsely_match_vhbs') }}

),

engaged_xf as (

  select
      vs.event_id,
      sum(vhb.engaged_time_inc) as engaged_time
  from videostart_events vhb
  left join {{ref('parsely_parent_videostart_keys')}} vs using (videostart_key)
  where vhb.action = 'vheartbeat' and 
  vhb.ts_action >= vs.ts_action and
  (case when vs.next_videostart_ts_action is not null
    then vhb.ts_action < vs.next_videostart_ts_action
    else true end)
  group by vs.event_id
)

select
  *
from videostart_events
left join engaged_xf using (event_id)
where action = 'videostart'
