{{
    config(
        materialized='ephemeral'
    )
}}

with incoming_users_pageviews as (
  select
      apikey,
      visitor_site_id,
      --custom fields
      apikey_visitor_id,
      -- metrics
      max(ts_session_current_tz) as last_timestamp,
      sum(pageviews) as user_total_pageviews,
      sum(engaged_time) as user_total_engaged_time,
      0 as user_total_videoviews,
      0 as user_total_video_engaged_time
  from {{ ref('parsely_pageviews_sessionized') }}
  group by 1,2,3
),

incoming_users_videostarts as (
  select
      apikey,
      visitor_site_id,
      --custom fields
      apikey_visitor_id,
      -- metrics
      max(ts_session_current_tz) as last_timestamp,
      0 as user_total_pageviews,
      0 as user_total_engaged_time,
      sum(videoviews) as user_total_videoviews,
      sum(video_engaged_time) as user_total_video_engaged_time
  from {{ ref('parsely_videoviews_sessionized') }}
  group by 1,2,3
)

select * from incoming_users_pageviews
union all
select * from incoming_users_videostarts
