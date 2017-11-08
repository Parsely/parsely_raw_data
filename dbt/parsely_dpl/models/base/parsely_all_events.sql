select distinct --disinct included due to duplicate rows and event_ids
      *,
    coalesce(metadata_canonical_url,url) as pageview_post_id,
    -- has keys pageview_key, videostart_key, utm_id, parsely_session_id
    md5(apikey || '_' || session_id || '_' || visitor_site_id || '_' || url ||
      '_' || metadata_canonical_url || '_' || referrer || '_' ||
      ts_session_current)         as videostart_key,
    md5(apikey || '_' || session_id || '_' || visitor_site_id || '_' ||
      coalesce(metadata_canonical_url,url) || '_' || referrer || '_' ||
      ts_session_current) as pageview_key,
    utm_campaign || '_' || utm_medium || '_' || utm_source || '_' ||
      utm_term || '_' || utm_content as utm_id,
    apikey || '_' || session_id || '_' || visitor_site_id || '_' ||
      session_timestamp as parsely_session_id,
    json_extract_path_text(
        extra_data,
        {{var('custom:extradata')}})     as {{var('custom:extradataname')}}
from {{ var('parsely:events') }}
