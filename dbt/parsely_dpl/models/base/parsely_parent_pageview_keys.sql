{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='pageview_incremental_key'
    )
}}

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
    md5(apikey || '_' || session_id || '_' || visitor_site_id || '_' || pageview_post_id || '_' || referrer || '_' || ts_session_current) as pageview_key,
    md5(apikey || '_' || session_id || '_' || visitor_site_id || '_' || pageview_post_id || '_' || referrer || '_' || ts_session_current || '_' || ts_action) as pageview_incremental_key
from {{ref('parsely_base_events')}}
where action in ('pageview')
