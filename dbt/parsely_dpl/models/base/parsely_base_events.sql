select
    *,
    coalesce(metadata_canonical_url,url) as pageview_post_id
from {{ var('parsely:events') }}
