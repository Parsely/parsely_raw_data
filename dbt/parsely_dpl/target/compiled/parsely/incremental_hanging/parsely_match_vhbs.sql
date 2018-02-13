

select
    *
from "blog_dbt_dev"."parsely_base_events"
where action in ('videostart','vheartbeat')
UNION all
select
  *
from "blog_dbt_dev"."parsely_vhbs_no_vs"
where videostart_key in
(select distinct videostart_key from "blog_dbt_dev"."parsely_base_events")