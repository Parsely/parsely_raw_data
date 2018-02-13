

select
    *
from "blog_dbt_dev"."parsely_base_events"
where action in ('pageview','heartbeat')
UNION all
select
  *
from "blog_dbt_dev"."parsely_hbs_no_pvs"
where pageview_key in
(select distinct pageview_key from "blog_dbt_dev"."parsely_base_events")