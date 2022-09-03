{{config(alias='transfers')}}

with erc20_transfers as (
  select
    from,
    to,
    cast(value as double) as amount,
    contract_address as token_address,
    evt_block_time as block_time,
    evt_tx_hash as tx_hash
  from {{ source('ethereum_common', 'erc_20_evt_transfer') }}
)

select *
from erc20_transfers