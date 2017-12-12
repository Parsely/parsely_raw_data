-- 1 row per session
-- Join from the parsely_pageviews and parsely_videovideos
-- aggregated: pageviews, engaged time, videoviews, video engaged time
-- should also have session visitor type, returning, new, subscribers, etc (what was true at the time of the session)
-- metrics: sessions, pageviws, videoviews, engaged time, video watch time

{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='parsely_session_id'
    )
}}


with session_metrics as (
  select
      parsely_session_id,
      sum(pageviews) as pageviews,
      sum(engaged_time) as pageview_engaged_time,
      sum(videoviews) as videoviews,
      sum(video_engaged_time) as video_engaged_time
  from {{ref('parsely_pageviews_sessionized')}}
  group by parsely_session_id
),

users as (
    select
      apikey_visitor_id,
      user_type,
      user_engagement_level
    from {{ref('parsely_users')}}
),

session_xf as (
  select  --add row number 1=1 here
  --  id
      parsely_session_id,
      apikey_visitor_id,
  --  session user dimensions
      user_type as session_user_type,
      user_engagement_level as session_user_engagement_level,
  --  counter field
      1 as session_counter,
  --  session time fields
      session_day,
      session_quarter,
      session_month,
      session_year,
      session_week,
      session_date_id,
      pv.{{ var('custom:extradataname') }},
      apikey	,
      flags_is_amp	,
      ip_city	,
      ip_continent	,
      ip_country	,
      ip_lat::FLOAT8	,
      ip_lon	,
      ip_postal	,
      ip_subdivision	,
      ip_timezone	,
      ip_market_name	,
      ip_market_nielsen	,
      ip_market_doubleclick	,
      session	,
      session_id	,
      session_initial_referrer	,
      session_initial_url	,
      session_last_session_timestamp	,
      session_timestamp	,
      slot	,
      sref_category	,
      sref_clean	,
      sref_domain	,
      sref_fragment	,
      sref_netloc	,
      sref_params	,
      sref_path	,
      sref_query	,
      sref_scheme	,
      surl_clean	,
      surl_domain	,
      surl_fragment	,
      surl_netloc	,
      surl_params	,
      surl_path	,
      surl_query	,
      surl_scheme	,
      ua_browser	,
      ua_browserversion	,
      ua_device	,
      ua_devicebrand	,
      ua_devicemodel	,
      ua_devicetouchcapable	,
      ua_devicetype	,
      ua_os	,
      ua_osversion	,
      user_agent	,
      version	,
      visitor	,
      visitor_ip	,
      visitor_network_id	,
      visitor_site_id
  from {{ref('parsely_pageviews_sessionized')}} as pv
  left join users using (apikey_visitor_id)
)

select
  *
from session_xf
left join session_metrics using (parsely_session_id)
