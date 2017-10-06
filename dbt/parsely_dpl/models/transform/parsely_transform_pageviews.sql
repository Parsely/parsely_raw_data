-- 1 row per pageview

with pageviews as (

    select * from parsely_base_events
    where action in ('pageview','heartbeat')

),

-- derived fields
pageviews_xf as (

    select *,
        (TIMESTAMP 'epoch' + left(metadata_pub_date_tmsp,10)::bigint * INTERVAL '1 Second ') as publish_time,
        (TIMESTAMP 'epoch' + left(timestamp_info_nginx_ms,10)::bigint * INTERVAL '1 Second ') as read_time

    from pageviews

)

-- aggregating engaged time
engaged_xf as (

  select
      apikey,
      session_id,
      visitor_site_id,
      metadata_post_id,
      -- date?? hour?? 5 min bucket? can't rely on session_id, resets probably every s3 bucket
      --join on interval of timestamp with ts_action and pageview and heartbeats
      --one session, visitor, and postid can have multipe pageviews per session - so need to include a time interval
      --doing it without interval match means potential duplicate counts of engaged time when summing
      sum(engaged_time_inc) as engaged_time
  from pageviews
  where action = 'heartbeat'
  group by apikey, session_id, visitor_site_id, metadata_post_id --, hour? or date?
)


select

    -- derived fields
    datediff(hour, publish_time, read_time) as hours_since_publish,
    datediff(day, publish_time, read_time) as days_since_publish,
    datediff(week, publish_time, read_time) as weeks_since_publish,
    -- aggregated fields
    exf.engaged_time,
    -- standard fields
    action	,
    apikey	,
    campaign_id	,
    display,
    display_avail_height	,
    display_avail_width	,
    display_pixel_depth	,
    display_total_height	,
    display_total_width	,
    event_id	,
    extra_data,
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
    metadata	,
    metadata_authors	,
    metadata_canonical_url	,
    metadata_custom_metadata	,
    metadata_duration	,
    metadata_data_source	,
    metadata_full_content_word_count	,
    metadata_image_url	,
    metadata_page_type	,
    metadata_post_id	,
    metadata_pub_date_tmsp	,
    metadata_save_date_tmsp	,
    metadata_section	,
    metadata_share_urls	,
    metadata_tags	,
    metadata_thumb_url	,
    metadata_title	,
    metadata_urls	,
    ref_category	,
    ref_clean	,
    ref_domain	,
    ref_fragment	,
    ref_netloc	,
    ref_params	,
    ref_path	,
    ref_query	,
    ref_scheme	,
    referrer	,
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
    timestamp_info	,
    timestamp_info_nginx_ms	,
    timestamp_info_override_ms	,
    timestamp_info_pixel_ms	,
    ts_action	,
    ts_session_current	,
    ts_session_last	,
    ua_browser	,
    ua_browserversion	,
    ua_device	,
    ua_devicebrand	,
    ua_devicemodel	,
    ua_devicetouchcapable	,
    ua_devicetype	,
    ua_os	,
    ua_osversion	,
    url	,
    url_clean	,
    url_domain	,
    url_fragment	,
    url_netloc	,
    url_params	,
    url_path	,
    url_query	,
    url_scheme	,
    utm_campaign	,
    utm_medium	,
    utm_source	,
    utm_term	,
    utm_content	,
    user_agent	,
    version	,
    visitor	,
    visitor_ip	,
    visitor_network_id	,
    visitor_site_id
  from pageviews pv
  where action = 'pageview'
  left join events_xf exf on
    exf.apikey = pv.apikey and
    exf.session_id = pv.session_id and
    exf.visitor_site_id = pv.visitor_site_id and
    exf.metadata_post_id = pv.metadata_post_id;
