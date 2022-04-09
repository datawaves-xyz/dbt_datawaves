with wyvern_data as (
  select * from {{ ref('wyvern_data') }}
)

select * from wyvern_data
