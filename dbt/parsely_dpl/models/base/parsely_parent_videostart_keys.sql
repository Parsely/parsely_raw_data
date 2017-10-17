select
    apikey,
    session_id,
    visitor_site_id,
    metadata_canonical_url,
    url,
    referrer,
    vs.ts_action,
    LAG(vs.ts_action, 1) OVER
      (PARTITION BY
         apikey,
         session_id,
         visitor_site_id,
         metadata_canonical_url,
         url,
         referrer
       ORDER BY vs.ts_action) AS previous_videostart_ts_action,
     LAG(vs.ts_action, 1) OVER
       (PARTITION BY
         apikey,
         session_id,
         visitor_site_id,
         metadata_canonical_url,
         url,
         referrer
      ORDER BY vs.ts_action desc) AS next_videostart_ts_action,
--  hash keys
    pageview_key,
    md5(apikey || '_' || session_id || '_' || visitor_site_id || '_' || url || '_' || metadata_canonical_url || '_' || referrer) as videostart_key
from {{ref('parsely_base_events')}} vs
left join {{ ref('parsely_parent_pageview_keys')}} pv using (apikey, session_id, referrer, visitor_site_id, url)
where action in ('videostart')
and vs.ts_action >= pv.ts_action and (case when pv.next_pageview_ts_action is not null then vs.ts_action < pv.next_pageview_ts_action else true end)
