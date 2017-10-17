select
    *,
--  hash keys
    md5(apikey || '_' || session_id || '_' || visitor_site_id || '_' || coalesce(metadata_canonical_url,url) || '_' || referrer || '_' || ts_action) as pv_key

from {{ var('parsely:events') }}
