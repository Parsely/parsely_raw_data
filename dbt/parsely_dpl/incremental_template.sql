{{
    config(
        materialized='incremental',
        sql_where='TRUE',
        unique_key='id'
    )
}}

with incoming_data as (
  select
      *
  from {{ref('parsely_incoming_data')}}
),

{%if adapter.already_exists(this.schema,this.name)%}

relevant_existing as (

    select
        *
    from {{ this }}
    where id in (select id from incoming_data)

),

-- left join fields from old data: min_tstamp
unioned as (

    -- combined pageviews and videostarts
    select
      *
    from incoming_data

    union all

    select
        *
    from relevant_existing

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
    from incoming_data
)

{% endif %}

select
    * --and derviced fields
from merged
