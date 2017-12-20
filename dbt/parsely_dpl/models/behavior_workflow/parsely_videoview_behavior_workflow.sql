-- 1 row per videoview
-- sum engaged time for all heartbeats
-- metrics: videoviews, engaged time

{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='event_id'
    )
}}

with video_events as (

    select * from {{ ref('parsely_videoview_engagedtime') }}
),

-- derived fields
publish_watch_time_xf as (
    select
        event_id,
        (TIMESTAMP 'epoch'
          + left(metadata_pub_date_tmsp_tz,10)::bigint
          * INTERVAL '1 Second ') as publish_time,
        (TIMESTAMP 'epoch'
          + left(timestamp_info_nginx_ms_tz,10)::bigint
          * INTERVAL '1 Second ') as watch_time

    from video_events

)

select
    -- aggregated fields
    engaged_time as video_engaged_time,
    1 as videostart_counter,
    -- derived fields
    datediff(hour, publish_time, watch_time) as hours_since_publish,
    datediff(day, publish_time, watch_time) as days_since_publish,
    datediff(week, publish_time, watch_time) as weeks_since_publish,
    case
      when engaged_time > {{ var('custom:videodeepwatchtime') }} then 'Deep Watch'
      when engaged_time > {{ var('custom:videoskimtime') }} then 'Watch'
      else 'Skim' end as watch_category,
    publish_time,
    watch_time,
    {{ var('custom:extradataname') }},
    pageview_key,
    videostart_key,
    parsely_session_id,
    utm_id,
    apikey_visitor_id,
    -- event time fields
    quarter,
    month,
    year,
    week,
    date_id,
    session_date_id,
    -- standard fields
    action,
    apikey,
    campaign_id,
    display,
    display_avail_height,
    display_avail_width,
    display_pixel_depth,
    display_total_height,
    display_total_width,
    event_id,
    extra_data,
    flags_is_amp,
    flag_is_fbia,
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
    timestamp_info,
    timestamp_info_nginx_ms,
    timestamp_info_override_ms,
    timestamp_info_pixel_ms,
    ts_action,
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
  from video_events
  left join publish_watch_time_xf using (event_id)
