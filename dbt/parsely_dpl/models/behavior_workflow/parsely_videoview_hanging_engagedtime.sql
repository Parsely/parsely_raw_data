
with hanging_engaged as (
  SELECT
    *
  from {{ ref('parsely_base_events') }}
  where action in ('vheartbeat')
  and videostart_key not in
    (select distinct videostart_key from {{ref('parsely_parent_videostart_keys')}})
),

first_timestamp as (
  SELECT
    min(ts_action) as ts_action,
    TRUE           as min_ts_flag,
    videostart_key
  from hanging_engaged
  group by videostart_key
)

SELECT
  event_id,
  videostart_key,
  min_ts_flag,
  engaged_time_inc
from hanging_engaged
left join first_timestamp using (videostart_key, ts_action)
