select
    *
from {{ ref('parsely_all_events') }}
where action in {{ var('parsely:actions') }}
and event_id not in
  (select event_id from {{ref('parsely_event_ids')}})
and ua_browser <> 'Googlebot'
--add in logic for custom:excludebottraffic== 'Yes'
