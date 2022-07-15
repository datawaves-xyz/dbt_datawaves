
with final as (
  select *
  from {{ ref('opensea_ethereum_trades') }}

  union all

  select *
  from {{ ref('cryptopunks_ethereum_trades') }}
)

select *
from final
