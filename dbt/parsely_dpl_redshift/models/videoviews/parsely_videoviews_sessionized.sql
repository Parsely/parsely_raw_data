{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='videostart_key'
    )
}}

with incoming_videoviews as (

  SELECT
    *
  from {{ ref('parsely_incoming_videoviews') }}


),


{%if adapter.already_exists(this.schema,this.name)%}

relevant_existing as (

    select
        *
    from {{ this }}
    where videostart_key in (select videostart_key from incoming_videoviews)

),

-- left join fields from old data: min_tstamp
unioned as (

    select
      *
    from incoming_videoviews

    union all

    select
      *
    from relevant_existing

),

merged_aggr as (

    select
      sum(video_engaged_time) as engaged_time_unioned,
      sum(videoviews) as videoviews_unioned,
      case when sum(videoviews) = 0 then 0 else
         sum(video_engaged_time)/sum(videoviews) end as avg_video_engaged_time_unioned,
      videostart_key
    from unioned
    group by videostart_key
),

merged as (
    SELECT
    engaged_time_unioned as video_engaged_time,
    videoviews_unioned as videoviews,
    avg_video_engaged_time_unioned as avg_video_engaged_time,
    -- derived fields
    {{ var('custom:extradataname') }},
    pageview_post_id,
    watch_category,
    publish_time,
    watch_time,
    hours_since_publish,
    days_since_publish,
    weeks_since_publish,
    -- event time fields
    session_day,
    session_quarter,
    session_month,
    session_year,
    session_week,
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
  from incoming_videoviews
  left join merged_aggr using (videostart_key)
)

{% else %}

-- initial run, don't merge
merged as (

    select
      *
    from incoming_videoviews
)

{% endif %}

select
  *
from merged
