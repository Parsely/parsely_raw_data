with hanging_engaged as (
  SELECT
    *
  from {{ ref('parsely_base_events') }}
  where action in ('heartbeat')
  and pageview_key not in
    (select distinct pageview_key from {{ref('parsely_parent_pageview_keys')}})
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
