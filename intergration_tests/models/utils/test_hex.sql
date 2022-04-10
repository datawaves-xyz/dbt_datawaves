with data as (
  select * from {{ ref("data_hex")}}
)

select 

  {{ ethereum.hex('input') }} as actual,
  output as expected
from data