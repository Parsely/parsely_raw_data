

-- created to track event_ids for duplicate event_ids that do not need to be processed twice
-- how often should this truncate?
select distinct
  event_id
from "blog_dbt_dev"."parsely_base_events"