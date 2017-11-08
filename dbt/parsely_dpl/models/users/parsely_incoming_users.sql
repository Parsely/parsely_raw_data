-- 1 row per visitor_site_Id
-- includes visitor type, returning, new, subscribers, etc
-- first login, last login, etc

{{
    config(
        materialized='ephemeral'
    )
}}

with incoming_users_pageviews as (
  select
      apikey,
      visitor_site_id,
      visitor_ip,
      --custom fields
      apikey || '_' || visitor_site_id || '_' || visitor_ip as apikey_visitor_id,
      {{ var('custom:extradataname') }},
      -- metrics
      max(ts_action) as last_timestamp,
      sum(pageview_counter) as user_total_pageviews,
      sum(engaged_time) as user_total_engaged_time,
      0 as user_total_videoviews,
      0 as user_total_video_engaged_time
  from {{ ref('parsely_pageviews') }}
  group by apikey, visitor_site_id, visitor_ip, {{ var('custom:extradataname') }}
),

incoming_users_videostarts as (
  select distinct
      apikey,
      visitor_site_id,
      visitor_ip,
      --custom fields
      apikey || '_' || visitor_site_id || '_' || visitor_ip as apikey_visitor_id,
      {{ var('custom:extradataname') }},
      -- metrics
      max(ts_action) as last_timestamp,
      0 as user_total_pageviews,
      0 as user_total_engaged_time,
      sum(videostart_counter) as user_total_videoviews,
      sum(video_engaged_time) as user_total_video_engaged_time
  from {{ ref('parsely_videoviews') }}
  group by apikey, visitor_site_id, visitor_ip, {{ var('custom:extradataname') }}
)

select * from incoming_users_pageviews
union all
select * from incoming_users_videostarts
