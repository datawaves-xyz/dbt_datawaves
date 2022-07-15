
{{ config(
        alias ='trades',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id'
        )
}}


with final as (
  select *
  from {{ ref('opensea_ethereum_trades') }}

  union all

  select *
  from {{ ref('cryptopunks_ethereum_trades') }}
)

select *
from final

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where block_time > date_sub(current_date(), 2)
{% endif %} 