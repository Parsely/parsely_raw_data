  select
      *
  from {{ ref('parsely_all_events') }}
  where action in {{ var('parsely:actions') }}
{%if {{ref('custom:excludebottraffic')}} == 'Yes' %}
  and ua_browser <> 'Googlebot'
{% endif %}
