
with erc721_transfers as (
  select
    from,
    to,
    cast(token_id as {{ dbt_utils.type_string() }}) as token_id,
    cast(1 as double) as amount,
    contract_address as token_address,
    evt_block_time as block_time,
    evt_tx_hash as tx_hash
  from {{ source('ethereum_common', 'erc_721_evt_transfer') }}
)

select *
from erc721_transfers