create view "blog_dbt_dev"."parsely_pageview_engagedtime__dbt_tmp" as (
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
),pageview_events as (

    select * from "blog_dbt_dev"."parsely_base_events"
    where action in ('pageview','heartbeat')

),

videoview_events as (
    SELECT
      sum(videostart_counter) as videoviews,
      sum(video_engaged_time) as video_engaged_time,
      pageview_key
    from "blog_dbt_dev"."parsely_videoview_behavior_workflow"
    group by pageview_key
),

engaged_xf as (

-- join videoviews and vheartbeats when they match up
  select
      pv.event_id,
      hb.pageview_key,
      true as pageview_match,
      false as min_ts_flag,
      sum(hb.engaged_time_inc) as engaged_time
  from pageview_events hb
  left join __dbt__CTE__parsely_parent_pageview_keys pv using (pageview_key)
  where hb.action = 'heartbeat' and
  hb.ts_action >= pv.ts_action and
  (case when pv.next_pageview_ts_action is not null
    then hb.ts_action < pv.next_pageview_ts_action
    else true end)
  group by pv.event_id, hb.pageview_key
),

engaged_no_matches_aggr as (
-- aggregated engaged time when videoviews and vheartbeats do not match up
-- using the ts_action and metadata from the first heartbeat
  select
      sum(engaged_time_inc) as engaged_time,
      pageview_key
  from "blog_dbt_dev"."parsely_pageview_hanging_engagedtime"
  group by pageview_key
),

engaged_no_matches as (
  SELECT
    event_id,
    pageview_key,
    false as pageview_match,
    min_ts_flag,
    engaged_time
  from "blog_dbt_dev"."parsely_pageview_hanging_engagedtime"
  left join engaged_no_matches_aggr using (pageview_key)
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
from pageview_events pv
  left join unioned using (event_id, pageview_key)
  left join videoview_events using (pageview_key)
where (pv.action = 'pageview' or min_ts_flag is true)
and (min_ts_flag is true or pageview_match is true)
  );