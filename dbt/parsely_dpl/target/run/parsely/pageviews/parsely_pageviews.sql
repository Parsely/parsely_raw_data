
       

       delete
  from "blog_dbt_dev"."parsely_pageviews"
  where (event_id) in (
    select (event_id)
    from "parsely_pageviews__dbt_incremental_tmp"
  );

       insert into "blog_dbt_dev"."parsely_pageviews" ("visitor", "ua_devicetouchcapable", "timestamp_info", "slot", "session", "metadata", "flags_is_amp", "display", "timestamp_info_pixel_ms", "timestamp_info_override_ms", "timestamp_info_nginx_ms", "session_timestamp", "session_last_session_timestamp", "metadata_save_date_tmsp", "metadata_pub_date_tmsp", "weeks_since_publish", "days_since_publish", "hours_since_publish", "videoviews", "video_engaged_time", "engaged_time", "version", "session_id", "metadata_full_content_word_count", "metadata_duration", "display_total_width", "display_total_height", "display_pixel_depth", "display_avail_width", "display_avail_height", "session_date_id", "date_id", "pageview_counter", "ip_lon", "ip_lat", "week", "year", "month", "quarter", "day", "visitor_site_id", "visitor_network_id", "visitor_ip", "user_agent", "utm_content", "utm_term", "utm_source", "utm_medium", "utm_campaign", "url_scheme", "url_query", "url_path", "url_params", "url_netloc", "url_fragment", "url_domain", "url_clean", "url", "ua_osversion", "ua_os", "ua_devicetype", "ua_devicemodel", "ua_devicebrand", "ua_device", "ua_browserversion", "ua_browser", "surl_scheme", "surl_query", "surl_path", "surl_params", "surl_netloc", "surl_fragment", "surl_domain", "surl_clean", "sref_scheme", "sref_query", "sref_path", "sref_params", "sref_netloc", "sref_fragment", "sref_domain", "sref_clean", "sref_category", "session_initial_url", "session_initial_referrer", "referrer", "ref_scheme", "ref_query", "ref_path", "ref_params", "ref_netloc", "ref_fragment", "ref_domain", "ref_clean", "ref_category", "metadata_urls", "metadata_title", "metadata_thumb_url", "metadata_tags", "metadata_share_urls", "metadata_section", "metadata_post_id", "metadata_page_type", "metadata_image_url", "metadata_data_source", "metadata_custom_metadata", "metadata_canonical_url", "metadata_authors", "ip_market_doubleclick", "ip_market_nielsen", "ip_market_name", "ip_timezone", "ip_subdivision", "ip_postal", "ip_country", "ip_continent", "ip_city", "extra_data", "event_id", "campaign_id", "apikey", "action", "apikey_visitor_id", "utm_id", "parsely_session_id", "pageview_key", "pageview_post_id", "customer_apikey", "ts_session_last", "ts_session_current", "ts_action", "read_time", "publish_time")
       (
         select "visitor", "ua_devicetouchcapable", "timestamp_info", "slot", "session", "metadata", "flags_is_amp", "display", "timestamp_info_pixel_ms", "timestamp_info_override_ms", "timestamp_info_nginx_ms", "session_timestamp", "session_last_session_timestamp", "metadata_save_date_tmsp", "metadata_pub_date_tmsp", "weeks_since_publish", "days_since_publish", "hours_since_publish", "videoviews", "video_engaged_time", "engaged_time", "version", "session_id", "metadata_full_content_word_count", "metadata_duration", "display_total_width", "display_total_height", "display_pixel_depth", "display_avail_width", "display_avail_height", "session_date_id", "date_id", "pageview_counter", "ip_lon", "ip_lat", "week", "year", "month", "quarter", "day", "visitor_site_id", "visitor_network_id", "visitor_ip", "user_agent", "utm_content", "utm_term", "utm_source", "utm_medium", "utm_campaign", "url_scheme", "url_query", "url_path", "url_params", "url_netloc", "url_fragment", "url_domain", "url_clean", "url", "ua_osversion", "ua_os", "ua_devicetype", "ua_devicemodel", "ua_devicebrand", "ua_device", "ua_browserversion", "ua_browser", "surl_scheme", "surl_query", "surl_path", "surl_params", "surl_netloc", "surl_fragment", "surl_domain", "surl_clean", "sref_scheme", "sref_query", "sref_path", "sref_params", "sref_netloc", "sref_fragment", "sref_domain", "sref_clean", "sref_category", "session_initial_url", "session_initial_referrer", "referrer", "ref_scheme", "ref_query", "ref_path", "ref_params", "ref_netloc", "ref_fragment", "ref_domain", "ref_clean", "ref_category", "metadata_urls", "metadata_title", "metadata_thumb_url", "metadata_tags", "metadata_share_urls", "metadata_section", "metadata_post_id", "metadata_page_type", "metadata_image_url", "metadata_data_source", "metadata_custom_metadata", "metadata_canonical_url", "metadata_authors", "ip_market_doubleclick", "ip_market_nielsen", "ip_market_name", "ip_timezone", "ip_subdivision", "ip_postal", "ip_country", "ip_continent", "ip_city", "extra_data", "event_id", "campaign_id", "apikey", "action", "apikey_visitor_id", "utm_id", "parsely_session_id", "pageview_key", "pageview_post_id", "customer_apikey", "ts_session_last", "ts_session_current", "ts_action", "read_time", "publish_time"
         from "parsely_pageviews__dbt_incremental_tmp"
       );
     