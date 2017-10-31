

select distinct --due to duplicate rows and event_ids
    *,
    coalesce(metadata_canonical_url,url) as pageview_post_id,
    json_extract_path_text(extra_data, 'userType') as {{ var('custom:extradataname') }}
from {{ var('parsely:events') }}
