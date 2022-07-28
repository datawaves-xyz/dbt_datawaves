
with erc721_transfers as (
  select *
  from {{ source('ethereum_common', 'erc_721_evt_transfer') }}
),

sent_transfers as (
  select
    from as wallet_address,
    contract_address as token_address,
    evt_block_time as block_time,
    token_id,
    -1 as amount
  from erc721_transfers
),

received_transfers as (
  select
    to as wallet_address,
    contract_address as token_address,
    evt_block_time as block_time,
    token_id,
    1 as amount
  from erc721_transfers
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