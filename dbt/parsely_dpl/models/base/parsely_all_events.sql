{{
    config(
        materialized='incremental',
        sql_where = 'TRUE',
        unique_key='event_id'
    )
}}

with new_events as (

    select *
    from {{ ref('parsely_rawdata') }}

    {% if adapter.already_exists(this.schema, this.name) %}
    where insert_timestamp > (
        select coalesce(max(t.insert_timestamp), '0001-01-01') from {{ this }} as t
    )
    {% endif %}

),

timezone_convert as (
    SELECT
        *,
--      ts_action
        convert_timezone({{ var('parsely:timezone') }}, ts_action) as ts_action_tz,
--      ts_session_current
        convert_timezone({{ var('parsely:timezone') }}, ts_session_current) as ts_session_current_tz,
--      ts_session_last
        convert_timezone({{ var('parsely:timezone') }}, ts_session_last) as ts_session_last_tz,
--      metadata_pub_date_tmsp
        convert_timezone({{ var('parsely:timezone') }}, (TIMESTAMP 'epoch'
          + left(metadata_pub_date_tmsp,10)::bigint
          * INTERVAL '1 Second ')) as metadata_pub_date_tmsp_tz,
--      metadata_save_date_tmsp
        convert_timezone({{ var('parsely:timezone') }}, (TIMESTAMP 'epoch'
          + left(metadata_save_date_tmsp,10)::bigint
          * INTERVAL '1 Second ')) as metadata_save_date_tmsp_tz,
--      timestamp_info_nginx_ms
        convert_timezone({{ var('parsely:timezone') }}, (TIMESTAMP 'epoch'
          + left(timestamp_info_nginx_ms,10)::bigint
          * INTERVAL '1 Second ')) as timestamp_info_nginx_ms_tz,
--      session_last_session_timestamp
        convert_timezone({{ var('parsely:timezone') }}, (TIMESTAMP 'epoch'
          + left(session_last_session_timestamp,10)::bigint
          * INTERVAL '1 Second ')) as session_last_session_timestamp_tz,
--      session_timestamp
        convert_timezone({{ var('parsely:timezone') }}, (TIMESTAMP 'epoch'
          + left(session_timestamp,10)::bigint
          * INTERVAL '1 Second ')) as session_timestamp_tz,
--      timestamp_info_pixel_ms
        convert_timezone({{ var('parsely:timezone') }}, (TIMESTAMP 'epoch'
          + left(timestamp_info_pixel_ms,10)::bigint
          * INTERVAL '1 Second ')) as timestamp_info_pixel_ms_tz
    from new_events
),


dedupe as (
  select
      *,
  --  event action dates and times
      DATE_PART('day',ts_action_tz) as day,
      DATE_PART('quarter',ts_action_tz) as quarter,
      DATE_PART('month',ts_action_tz) as month,
      DATE_PART('year',ts_action_tz) as year,
      DATE_PART('week',ts_action_tz) as week,
      (DATE_PART('y', ts_action_tz)*10000+DATE_PART('mon', ts_action_tz)*100+DATE_PART('day', ts_action_tz))::int AS date_id,
      (DATE_PART('y', ts_session_current_tz)*10000+DATE_PART('mon', ts_session_current_tz)*100+DATE_PART('day', ts_session_current_tz))::int AS session_date_id,
  --  transformed fields
      coalesce(metadata_canonical_url,url) as pageview_post_id,
      json_extract_path_text(
          extra_data,
          {{var('custom:extradata')}})     as {{var('custom:extradataname')}},
      case when referrer = 'http://facebook.com/instantarticles'
        then true else false end as flag_is_fbia,
  --  dedupe field as we can receive duplicate event_ids that can be excluded
      row_number() over (partition by event_id order by ts_action) as n,
  --  counter fields
      case when action = 'pageview' then 1 else 0 end as pageview_counter,
      case when action = 'videostart' then 1 else 0 end as videostart_counter,
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
        coalesce(visitor_site_id,''))           as apikey_visitor_id
  from timezone_convert
)

select
  *
from dedupe
where n = 1
