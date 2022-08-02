
with received_transfers as (
  select
    to as wallet_address,
    token_address,
    block_time,
    token_id,
    amount
  from {{ ref("erc1155_ethereum_transfers") }}
),

sent_transfers as (
  select 
    from as wallet_address,
    token_address,
    block_time,
    token_id,
    -amount as amount
  from {{ ref("erc1155_ethereum_transfers") }}
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