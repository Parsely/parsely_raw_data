
with dedupe as (
  select
      *,
  --  event action dates and times
      DATE_PART('day',ts_action) as day,
      DATE_PART('quarter',ts_action) as quarter,
      DATE_PART('month',ts_action) as month,
      DATE_PART('year',ts_action) as year,
      DATE_PART('week',ts_action) as week,
      (DATE_PART('y', ts_action)*10000+DATE_PART('mon', ts_action)*100+DATE_PART('day', ts_action))::int AS date_id,
  --    DATE_PART('h',ts_action) as hour,
  --    DATE_PART('m',ts_action) as minute,
  --    DATE_PART('s',ts_action) as second,
  --  other date IDs for joining to the calendar dimension
      (DATE_PART('y', ts_session_current)*10000+DATE_PART('mon', ts_session_current)*100+DATE_PART('day', ts_session_current))::int AS session_date_id,
  --  transformed fields
      coalesce(metadata_canonical_url,url) as pageview_post_id,
      json_extract_path_text(
          extra_data,
          {{var('custom:extradata')}})     as {{var('custom:extradataname')}},
  --  dedupe field as we can receive duplicate event_ids that can be excluded
      row_number() over (partition by event_id order by ts_action) as n,
  --  hash identifier fields
      md5(
        coalesce(apikey,'') || '_' ||
        coalesce(session_id::text,'') || '_' ||
        coalesce(visitor_site_id,'') || '_' ||
        coalesce(url,'') || '_' ||
        coalesce(metadata_canonical_url,'') || '_' ||
        coalesce(referrer,'') || '_' ||
        coalesce(ts_session_current::text,''))         as videostart_key,
     md5(
        coalesce(apikey,'') || '_' ||
        coalesce(session_id::text,'') || '_' ||
        coalesce(visitor_site_id,'') || '_' ||
        coalesce(metadata_canonical_url,url) || '_' ||
        coalesce(referrer,'') || '_' ||
        coalesce(ts_session_current::text,''))         as pageview_key,
      md5(
        coalesce(apikey,'') || '_' ||
        coalesce(utm_campaign,'') || '_' ||
        coalesce(utm_medium,'') || '_' ||
        coalesce(utm_source ,'') || '_' ||
        coalesce(utm_term,'') || '_' ||
        coalesce(utm_content,'') )               as utm_id,
      md5(
        coalesce(apikey,'') || '_' ||
        coalesce(session_id::text,'') || '_' ||
        coalesce(visitor_site_id,'') || '_' ||
        coalesce(session_timestamp::text,''))            as parsely_session_id,
      md5(
        coalesce(apikey,'') || '_' ||
        coalesce(visitor_ip,'') || '_' ||
        coalesce(visitor_site_id,''))           as apikey_visitor_id
  from {{ var('parsely:events') }}
)

select
  *
from dedupe
where n = 1
