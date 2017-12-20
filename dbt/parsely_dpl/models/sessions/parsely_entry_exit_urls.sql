with sessions_time_xf as (
  select
      parsely_session_id,
      max(ts_action_tz) as last_ts_action,
      min(ts_action_tz) as first_ts_action
  from {{ref('parsely_base_events')}}
  group by parsely_session_id
),

entry_url as (
  SELECT
    st.parsely_session_id,
    url         as entry_url,
    url_clean   as entry_url_clean,
    url_domain  as entry_url_domain,
    url_fragment as entry_url_fragment,
    url_netloc as entry_url_netloc,
    url_params as entry_url_params,
    url_path as entry_url_path,
    url_query as entry_url_query,
    url_scheme as entry_url_scheme,
    ts_action_tz as entry_ts_action
  from {{ref('parsely_base_events')}} be
  inner join sessions_time_xf st
    on be.parsely_session_id = st.parsely_session_id
    and be.ts_action_tz = st.first_ts_action_tz
),



exit_url as (
  SELECT
    st.parsely_session_id,
    url         as exit_url,
    url_clean   as exit_url_clean,
    url_domain  as exit_url_domain,
    url_fragment as exit_url_fragment,
    url_netloc as exit_url_netloc,
    url_params as exit_url_params,
    url_path as exit_url_path,
    url_query as exit_url_query,
    url_scheme as exit_url_scheme,
    ts_action_tz as exit_ts_action
  from {{ref('parsely_base_events')}} be
  inner join sessions_time_xf st
    on be.parsely_session_id = st.parsely_session_id
    and be.ts_action_tz = st.last_ts_action_tz

)

SELECT
  *
from sessions_time_xf
join entry_url using (parsely_session_id)
join exit_url using (parsely_session_id)
