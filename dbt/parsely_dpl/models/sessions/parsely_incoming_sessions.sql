-- 1 row per session
-- Join from the parsely_pageviews and parsely_videovideos
-- aggregated: pageviews, engaged time, videoviews, video engaged time
-- should also have session visitor type, returning, new, subscribers, etc (what was true at the time of the session)
-- metrics: sessions, pageviws, videoviews, engaged time, video watch time

with session_metrics as (
  select
      parsely_session_id,
      sum(pageviews) as pageviews,
      sum(engaged_time) as engaged_time,
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

entry_exit as (
  SELECT
    *
  from {{ref('parsely_entry_exit_urls')}}
),

session_dedupe_xf as (
  select  --add row number 1=1 here
      row_number() over (partition by parsely_session_id order by pageview_key) as n,
  --  id
      parsely_session_id,
      apikey_visitor_id,
  --  session user dimensions
      user_type as session_user_type,
      user_engagement_level as session_user_engagement_level,
  --  counter field
      1 as session_counter,
  --  derived fields
      flag_is_fbia,
      ts_session_current_tz,
      ts_session_last_tz,
      session_last_session_timestamp_tz,
      session_timestamp_tz,
  --  parsely_entry_exit_urls
      entry_url,
      entry_url_clean,
      entry_url_domain,
      entry_url_fragment,
      entry_url_netloc,
      entry_url_params,
      entry_url_path,
      entry_url_query,
      entry_url_scheme,
      entry_ts_action,
      exit_url,
      exit_url_clean,
      exit_url_domain,
      exit_url_fragment,
      exit_url_netloc,
      exit_url_params,
      exit_url_path,
      exit_url_query,
      exit_url_scheme,
      exit_ts_action,
  --  session time fields
      session_day,
      session_quarter,
      session_month,
      session_year,
      session_week,
      session_date_id,
  --  standard fields
      apikey,
      flags_is_amp,
      ip_city,
      ip_continent,
      ip_country,
      ip_lat::FLOAT8,
      ip_lon,
      ip_postal,
      ip_subdivision,
      ip_timezone,
      ip_market_name,
      ip_market_nielsen,
      ip_market_doubleclick,
      session,
      session_id,
      session_initial_referrer,
      session_initial_url,
      session_last_session_timestamp,
      session_timestamp,
      slot,
      sref_category,
      sref_clean,
      sref_domain,
      sref_fragment,
      sref_netloc,
      sref_params,
      sref_path,
      sref_query,
      sref_scheme,
      surl_clean,
      surl_domain,
      surl_fragment,
      surl_netloc,
      surl_params,
      surl_path,
      surl_query,
      surl_scheme,
      ua_browser,
      ua_browserversion,
      ua_device,
      ua_devicebrand,
      ua_devicemodel,
      ua_devicetouchcapable,
      ua_devicetype,
      ua_os,
      ua_osversion,
      user_agent,
      version,
      visitor,
      visitor_ip,
      visitor_network_id,
      visitor_site_id
  from {{ref('parsely_pageviews_sessionized')}} as pv
  left join users using (apikey_visitor_id)
  left join entry_exit using (parsely_session_id)
)

select
  *
from session_dedupe_xf
left join session_metrics using (parsely_session_id)
where n = 1
