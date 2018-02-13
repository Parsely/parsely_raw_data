with  __dbt__CTE__parsely_parent_pageview_keys as (


select
    apikey,
    session_id,
    visitor_site_id,
    pageview_post_id,
    pageview_post_id as url,
    referrer,
    ts_session_current,
    ts_action,
    event_id,
    LAG(ts_action, 1) OVER
      (PARTITION BY
         apikey,
         session_id,
         visitor_site_id,
         pageview_post_id,
         referrer,
         ts_session_current
       ORDER BY ts_action) AS previous_pageview_ts_action,
     LAG(ts_action, 1) OVER
       (PARTITION BY
         apikey,
         session_id,
         visitor_site_id,
         pageview_post_id,
         referrer,
         ts_session_current
      ORDER BY ts_action desc) AS next_pageview_ts_action,
--  hash keys
    pageview_key
from "blog_dbt_dev"."parsely_base_events"
where action in ('pageview')
),  __dbt__CTE__parsely_parent_videostart_keys as (


select
    apikey,
    session_id,
    visitor_site_id,
    metadata_canonical_url,
    url,
    referrer,
    ts_session_current,
    vs.event_id,
    vs.ts_action,
    LAG(vs.ts_action, 1) OVER
      (PARTITION BY
         apikey,
         session_id,
         visitor_site_id,
         metadata_canonical_url,
         url,
         referrer,
         ts_session_current
       ORDER BY vs.ts_action) AS previous_videostart_ts_action,
     LAG(vs.ts_action, 1) OVER
       (PARTITION BY
         apikey,
         session_id,
         visitor_site_id,
         metadata_canonical_url,
         url,
         referrer,
         ts_session_current
      ORDER BY vs.ts_action desc) AS next_videostart_ts_action,
--  hash keys
    pv.pageview_key,
    videostart_key
from "blog_dbt_dev"."parsely_base_events" vs
left join __dbt__CTE__parsely_parent_pageview_keys pv using (pageview_key, apikey, session_id, referrer, visitor_site_id, url, ts_session_current)
where action in ('videostart')
and vs.ts_action >= pv.ts_action and (case when pv.next_pageview_ts_action is not null then vs.ts_action < pv.next_pageview_ts_action else true end)
),videostart_events as (

    select * from "blog_dbt_dev"."parsely_base_events"
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
  left join __dbt__CTE__parsely_parent_videostart_keys vs using (videostart_key)
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
  from "blog_dbt_dev"."parsely_videoview_hanging_engagedtime"
  group by videostart_key
),

engaged_no_matches as (
  SELECT
    event_id,
    videostart_key,
    false as videostart_match,
    min_ts_flag,
    engaged_time
  from "blog_dbt_dev"."parsely_videoview_hanging_engagedtime"
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