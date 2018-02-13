create view "blog_dbt_dev"."parsely_pageview_hanging_engagedtime__dbt_tmp" as (
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
),hanging_engaged as (
  SELECT
    *
  from "blog_dbt_dev"."parsely_base_events"
  where action in ('heartbeat')
  and pageview_key not in
    (select distinct pageview_key from __dbt__CTE__parsely_parent_pageview_keys)
),

first_timestamp as (
  SELECT
    min(ts_action) as ts_action,
    TRUE           as min_ts_flag,
    pageview_key
  from hanging_engaged
  group by pageview_key
)

SELECT
  event_id,
  min_ts_flag,
  pageview_key,
  engaged_time_inc
from hanging_engaged
left join first_timestamp using (pageview_key, ts_action)
  );