
with erc20_transfers as (
  select *
  from {{ source('ethereum_common', 'erc_20_evt_transfer') }}
),

sent_transfers as (
  select
    from as wallet_address,
    contract_address as token_address,
    evt_block_time as block_time,
    -value as amount
  from erc20_transfers
),

received_transfers as (
  select
    to as wallet_address,
    contract_address as token_address,
    evt_block_time as block_time,
    value as amount
  from erc20_transfers
),

transfers as (
  select *
  from sent_transfers

  union

  select *
  from received_transfers
)

select 'Ethereum' as blockchain, *
from transfers