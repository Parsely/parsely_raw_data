-- 1 row per session

{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='parsely_session_id'
    )
}}


with incoming_sessions as (
  select
    *
  from {{ref('parsely_incoming_sessions')}}
),


{%if adapter.already_exists(this.schema,this.name)
  and not flags.FULL_REFRESH %}

relevant_existing_entry as (

    select
      parsely_session_id,
      entry_url,
      entry_url_clean,
      entry_url_domain,
      entry_url_fragment,
      entry_url_netloc,
      entry_url_params,
      entry_url_path,
      entry_url_query,
      entry_url_scheme,
      entry_ts_action
    from {{ this }}
    where parsely_session_id in (select parsely_session_id from incoming_sessions)

),

relevant_existing_exit as (

    select
      parsely_session_id,
      exit_url,
      exit_url_clean,
      exit_url_domain,
      exit_url_fragment,
      exit_url_netloc,
      exit_url_params,
      exit_url_path,
      exit_url_query,
      exit_url_scheme,
      exit_ts_action
    from {{ this }}
    where parsely_session_id in (select parsely_session_id from incoming_sessions)

),
-- left join fields from old data: min_tstamp
unioned as (

    -- combined pageviews and videostarts
    select
    --  session_metrics
        pageviews,
        engaged_time,
        videoviews,
        video_engaged_time,
    --  id
        parsely_session_id,
        apikey_visitor_id,
    --  session user dimensions
        session_user_type,
        session_user_engagement_level,
    --  counter field
        1 as session_counter,
    --  derived fields
        flag_is_fbia,
        ts_session_current_tz,
        ts_session_last_tz,
        session_last_session_timestamp_tz,
        session_timestamp_tz,
    --  entry/exit update logic
      case when entry.entry_ts_action < id.entry_ts_action
        then entry.entry_url else id.entry_url_path end as entry_url,
        case when entry.entry_ts_action < id.entry_ts_action
          then entry.entry_url_clean else id.entry_url_clean end as entry_url_clean,
        case when entry.entry_ts_action < id.entry_ts_action
          then entry.entry_url_domain else id.entry_url_domain end as entry_url_domain,
        case when entry.entry_ts_action < id.entry_ts_action
          then entry.entry_url_fragment else id.entry_url_fragment end as entry_url_fragment,
        case when entry.entry_ts_action < id.entry_ts_action
          then entry.entry_url_netloc else id.entry_url_netloc end as entry_url_netloc,
        case when entry.entry_ts_action < id.entry_ts_action
          then entry.entry_url_params else id.entry_url_params end as entry_url_params,
        case when entry.entry_ts_action < id.entry_ts_action
          then entry.entry_url_path else id.entry_url_path end as entry_url_path,
        case when entry.entry_ts_action < id.entry_ts_action
          then entry.entry_url_query else id.entry_url_query end as entry_url_query,
        case when entry.entry_ts_action < id.entry_ts_action
          then entry.entry_url_scheme else id.entry_url_scheme end as entry_url_scheme,
        case when entry.entry_ts_action < id.entry_ts_action
          then entry.entry_ts_action else id.entry_ts_action end as entry_ts_action,
        case when exit.exit_ts_action > id.exit_ts_action
          then exit.exit_url else id.exit_url end as exit_url,
        case when exit.exit_ts_action > id.exit_ts_action
          then exit.exit_url_clean else id.exit_url_clean end as exit_url_clean,
        case when exit.exit_ts_action > id.exit_ts_action
          then exit.exit_url_domain else id.exit_url_domain end as exit_url_domain,
        case when exit.exit_ts_action > id.exit_ts_action
          then exit.exit_url_fragment else id.exit_url_fragment end as exit_url_fragment,
        case when exit.exit_ts_action > id.exit_ts_action
          then exit.exit_url_netloc else id.exit_url_netloc end as exit_url_netloc,
        case when exit.exit_ts_action > id.exit_ts_action
          then exit.exit_url_params else id.exit_url_params end as exit_url_params,
        case when exit.exit_ts_action > id.exit_ts_action
          then exit.exit_url_path else id.exit_url_path end as exit_url_path,
        case when exit.exit_ts_action > id.exit_ts_action
          then exit.exit_url_query else id.exit_url_query end as exit_url_query,
        case when exit.exit_ts_action > id.exit_ts_action
          then exit.exit_url_scheme else id.exit_url_scheme end as exit_url_scheme,
        case when exit.exit_ts_action > id.exit_ts_action
          then exit.exit_ts_action else id.exit_ts_action end as exit_ts_action,
    --  session time fields
        session_day,
        session_quarter,
        session_month,
        session_year,
        session_week,
        session_date_id,
        apikey,
        flags_is_amp,
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
        ua_browser,
        ua_browserversion,
        ua_device,
        ua_devicebrand,
        ua_devicemodel,
        ua_devicetouchcapable,
        ua_devicetype,
        ua_os,
        ua_osversion,
        user_agent,
        version,
        visitor,
        visitor_ip,
        visitor_network_id,
        visitor_site_id,
        n
    from incoming_sessions id
    left join relevant_existing_entry entry using (parsely_session_id)
    left join relevant_existing_exit exit using (parsely_session_id)
),

merged as (

    select
      * -- and aggregated min,max,sums
    from unioned


)

{% else %}

-- initial run, don't merge
merged as (

    select
      *
    from incoming_sessions
)

{% endif %}

select
    * --and derviced fields
from merged
