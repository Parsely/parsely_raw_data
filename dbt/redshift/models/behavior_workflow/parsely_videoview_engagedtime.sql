with videostart_events as (

    select * from {{ ref('parsely_base_events') }}
    where action in ('videostart','vheartbeat')

),

engaged_xf as (

-- join videoviews and vheartbeats when they match up
  select
      vs.event_id,
      vhb.videostart_key,
      true as videostart_match,
      false as min_ts_flag,
      sum(vhb.engaged_time_inc) as engaged_time
  from videostart_events vhb
  left join {{ref('parsely_parent_videostart_keys')}} vs using (videostart_key)
  where vhb.action = 'vheartbeat' and
  vhb.ts_action >= vs.ts_action and
  (case when vs.next_videostart_ts_action is not null
    then vhb.ts_action < vs.next_videostart_ts_action
    else true end)
  group by vs.event_id, vhb.videostart_key
),

engaged_no_matches_aggr as (
-- aggregated engaged time when videoviews and vheartbeats do not match up
-- using the ts_action and metadata from the first heartbeat
  select
      sum(engaged_time_inc) as engaged_time,
      videostart_key
  from {{ ref('parsely_videoview_hanging_engagedtime') }}
  group by videostart_key
),

engaged_no_matches as (
  SELECT
    event_id,
    videostart_key,
    false as videostart_match,
    min_ts_flag,
    engaged_time
  from {{ ref('parsely_videoview_hanging_engagedtime') }}
  left join engaged_no_matches_aggr using (videostart_key)
  where min_ts_flag is true
),

unioned as (
  select
    *
  from engaged_xf

  union all

  select
    *
  from engaged_no_matches

)

select
  *
  from videostart_events vs
  left join unioned using (event_id, videostart_key)
  where (vs.action = 'videostart' or min_ts_flag is true)
  and (min_ts_flag is true or videostart_match is true)
