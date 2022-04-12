with data as (
  select * from {{ ref("data_binary_to_address")}}
)

select 

  {{ ethereum.binary_to_address(ethereum.binary_literal("ffffffffffffffffffff")) }} as actual,
  address as expected
from data