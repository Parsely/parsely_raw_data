
with videoview_events as (
  SELECT
    *
  FROM  {{ ref('parsely_base_events') }}
  where action in ('videostart','vheartbeat')
),


incoming_videoviews_aggr as (
  SELECT
    sum(engaged_time_inc) as video_engaged_time,
    sum(videostart_counter) as videoviews,
    case when sum(videostart_counter) = 0 then 0 else
       sum(engaged_time_inc)/sum(videostart_counter) end as avg_video_engaged_time,
    videostart_key
  FROM videoview_events
  group by videostart_key
),

publish_watch_time_xf as (
    select
        event_id,
        (TIMESTAMP 'epoch'
          + left(metadata_pub_date_tmsp_tz,10)::bigint
          * INTERVAL '1 Second ') as publish_time,
        (TIMESTAMP 'epoch'
          + left(timestamp_info_nginx_ms_tz,10)::bigint
          * INTERVAL '1 Second ') as watch_time

    from videoview_events

),

dedupe_videoviews_sessionized as (
  select
    row_number() over (partition by videostart_key order by ts_action) as n,
    -- derived fields
    {{ var('custom:extradataname') }},
    pageview_post_id,
    publish_time,
    watch_time,
    -- event time fields
    DATE_PART('day',ts_session_current_tz) as session_day,
    DATE_PART('quarter',ts_session_current_tz) as session_quarter,
    DATE_PART('month',ts_session_current_tz) as session_month,
    DATE_PART('year',ts_session_current_tz) as session_year,
    DATE_PART('week',ts_session_current_tz) as session_week,
    session_date_id,
    -- derived fields
    flag_is_fbia,
    ts_session_current_tz,
    ts_session_last_tz,
    metadata_pub_date_tmsp_tz,
    metadata_save_date_tmsp_tz,
    session_last_session_timestamp_tz,
    session_timestamp_tz,
    -- keys
    pageview_key,
    videostart_key,
    parsely_session_id,
    utm_id,
    apikey_visitor_id,
    -- standard fields
    apikey,
    campaign_id,
    display,
    display_avail_height,
    display_avail_width,
    display_pixel_depth,
    display_total_height,
    display_total_width,
    extra_data,
    flags_is_amp,
    ip_city,
    ip_continent,
    ip_country,
    ip_lat,
    ip_lon,
    ip_postal,
    ip_subdivision,
    ip_timezone,
    ip_market_name,
    ip_market_nielsen,
    ip_market_doubleclick,
    metadata,
    metadata_authors,
    metadata_canonical_url,
    metadata_custom_metadata,
    metadata_duration,
    metadata_data_source,
    metadata_full_content_word_count,
    metadata_image_url,
    metadata_page_type,
    metadata_post_id,
    metadata_pub_date_tmsp,
    metadata_save_date_tmsp,
    metadata_section,
    metadata_share_urls,
    metadata_tags,
    metadata_thumb_url,
    metadata_title,
    metadata_urls,
    ref_category,
    ref_clean,
    ref_domain,
    ref_fragment,
    ref_netloc,
    ref_params,
    ref_path,
    ref_query,
    ref_scheme,
    referrer,
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
    ts_session_current,
    ts_session_last,
    ua_browser,
    ua_browserversion,
    ua_device,
    ua_devicebrand,
    ua_devicemodel,
    ua_devicetouchcapable,
    ua_devicetype,
    ua_os,
    ua_osversion,
    url,
    url_clean,
    url_domain,
    url_fragment,
    url_netloc,
    url_params,
    url_path,
    url_query,
    url_scheme,
    utm_campaign,
    utm_medium,
    utm_source,
    utm_term,
    utm_content,
    user_agent,
    version,
    visitor,
    visitor_ip,
    visitor_network_id,
    visitor_site_id
  from videoview_events
  left join publish_watch_time_xf using (event_id)
)

select
    video_engaged_time,
    videoviews,
    avg_video_engaged_time,
    -- derived fields
    {{ var('custom:extradataname') }},
    pageview_post_id,
    publish_time,
    watch_time,
    case
      when avg_video_engaged_time > {{ var('custom:videodeepwatchtime') }} then 'Deep Watch'
      when avg_video_engaged_time > {{ var('custom:videoskimtime') }} then 'Watch'
      else 'Skim' end as watch_category,
    datediff(hour, publish_time, watch_time) as hours_since_publish,
    datediff(day, publish_time, watch_time) as days_since_publish,
    datediff(week, publish_time, watch_time) as weeks_since_publish,
    -- event time fields
    DATE_PART('day',ts_session_current_tz) as session_day,
    DATE_PART('quarter',ts_session_current_tz) as session_quarter,
    DATE_PART('month',ts_session_current_tz) as session_month,
    DATE_PART('year',ts_session_current_tz) as session_year,
    DATE_PART('week',ts_session_current_tz) as session_week,
    session_date_id,
    -- derived fields
    flag_is_fbia,
    ts_session_current_tz,
    ts_session_last_tz,
    metadata_pub_date_tmsp_tz,
    metadata_save_date_tmsp_tz,
    session_last_session_timestamp_tz,
    session_timestamp_tz,
    -- keys
    pageview_key,
    videostart_key,
    parsely_session_id,
    utm_id,
    apikey_visitor_id,
    -- standard fields
    apikey,
    campaign_id,
    display,
    display_avail_height,
    display_avail_width,
    display_pixel_depth,
    display_total_height,
    display_total_width,
    extra_data,
    flags_is_amp,
    ip_city,
    ip_continent,
    ip_country,
    ip_lat,
    ip_lon,
    ip_postal,
    ip_subdivision,
    ip_timezone,
    ip_market_name,
    ip_market_nielsen,
    ip_market_doubleclick,
    metadata,
    metadata_authors,
    metadata_canonical_url,
    metadata_custom_metadata,
    metadata_duration,
    metadata_data_source,
    metadata_full_content_word_count,
    metadata_image_url,
    metadata_page_type,
    metadata_post_id,
    metadata_pub_date_tmsp,
    metadata_save_date_tmsp,
    metadata_section,
    metadata_share_urls,
    metadata_tags,
    metadata_thumb_url,
    metadata_title,
    metadata_urls,
    ref_category,
    ref_clean,
    ref_domain,
    ref_fragment,
    ref_netloc,
    ref_params,
    ref_path,
    ref_query,
    ref_scheme,
    referrer,
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
    ts_session_current,
    ts_session_last,
    ua_browser,
    ua_browserversion,
    ua_device,
    ua_devicebrand,
    ua_devicemodel,
    ua_devicetouchcapable,
    ua_devicetype,
    ua_os,
    ua_osversion,
    url,
    url_clean,
    url_domain,
    url_fragment,
    url_netloc,
    url_params,
    url_path,
    url_query,
    url_scheme,
    utm_campaign,
    utm_medium,
    utm_source,
    utm_term,
    utm_content,
    user_agent,
    version,
    visitor,
    visitor_ip,
    visitor_network_id,
    visitor_site_id
from dedupe_videoviews_sessionized
left join incoming_videoviews_aggr using (videostart_key)
where n = 1
