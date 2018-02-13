  select
      *
  from {{ ref('parsely_all_events') }}
  where action in {{ var('parsely:actions') }}
