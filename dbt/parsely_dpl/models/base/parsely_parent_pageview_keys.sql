select
    apikey,
    session_id,
    visitor_site_id,
    pageview_post_id,
    pageview_post_id as url, --for joining to the parsely_parent_videostart_keys table
    referrer,
    ts_action,
    LAG(ts_action, 1) OVER
      (PARTITION BY
         apikey,
         session_id,
         visitor_site_id,
         pageview_post_id,
         referrer
       ORDER BY ts_action) AS previous_pageview_ts_action,
     LAG(ts_action, 1) OVER
       (PARTITION BY
         apikey,
         session_id,
         visitor_site_id,
         pageview_post_id,
         referrer
      ORDER BY ts_action desc) AS next_pageview_ts_action,
--  hash keys
    md5(apikey || '_' || session_id || '_' || visitor_site_id || '_' || pageview_post_id || '_' || referrer) as pageview_key
from {{ref('parsely_base_events')}}
where action in ('pageview')
