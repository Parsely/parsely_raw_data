create view "blog_dbt_dev"."parsely_pageview_engagedtime__dbt_tmp" as (
    with  __dbt__CTE__parsely_match_hbs as (


select
    *
from "blog_dbt_dev"."parsely_base_events"
where action in ('pageview','heartbeat')
UNION all
select
  *
from "blog_dbt_dev"."parsely_hbs_no_pvs"
where pageview_key in
(select distinct pageview_key from "blog_dbt_dev"."parsely_base_events")
),  __dbt__CTE__parsely_parent_pageview_keys as (


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

    select * from __dbt__CTE__parsely_match_hbs

),

engaged_xf as (

  select
      pv.event_id,
      sum(hb.engaged_time_inc) as engaged_time
  from pageview_events hb
  left join __dbt__CTE__parsely_parent_pageview_keys pv using (pageview_key)
  where hb.action = 'heartbeat' and
    hb.ts_action >= pv.ts_action and
    (case when pv.next_pageview_ts_action is not null
      then hb.ts_action < pv.next_pageview_ts_action
      else true end)
  group by pv.event_id
),

video_xf as (
  select
    pageview_key,
    sum(video_engaged_time) as video_engaged_time,
    sum(videostart_counter) as videoviews
  from "blog_dbt_dev"."parsely_videoviews"
  group by pageview_key
)

select
  *
from pageview_events
left join engaged_xf using (event_id)
left join video_xf using (pageview_key)
where action = 'pageview'
  );