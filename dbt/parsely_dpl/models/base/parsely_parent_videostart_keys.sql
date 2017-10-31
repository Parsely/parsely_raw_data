{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='videostart_incremental_key'
    )
}}

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
    pageview_key,
    md5(apikey || '_' || session_id || '_' || visitor_site_id || '_' || url || '_' || metadata_canonical_url || '_' || referrer || '_' || ts_session_current) as videostart_key,
    md5(apikey || '_' || session_id || '_' || visitor_site_id || '_' || url || '_' || metadata_canonical_url || '_' || referrer || '_' || ts_session_current || '_' || vs.ts_action) as videostart_incremental_key
from {{ref('parsely_base_events')}} vs
left join {{ ref('parsely_parent_pageview_keys')}} pv using (apikey, session_id, referrer, visitor_site_id, url, ts_session_current)
where action in ('videostart')
and vs.ts_action >= pv.ts_action and (case when pv.next_pageview_ts_action is not null then vs.ts_action < pv.next_pageview_ts_action else true end)
