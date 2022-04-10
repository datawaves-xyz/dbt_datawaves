with data as (
  select * from {{ ref("data_binary_literal") }}
)

select 

  {{ ethereum.binary_literal('input') }} as actual,
  output as expected
from data