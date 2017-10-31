select
    *
from {{ ref('parsely_all_events') }}
where action in {{ var('parsely:actions') }}
and ua_browser <> 'Googlebot'
--add in logic for custom:excludebottraffic== 'Yes'
