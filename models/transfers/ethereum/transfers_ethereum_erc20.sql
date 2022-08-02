with sent_transfers as (
  select
    from as wallet_address,
    token_address,
    block_time,
    -amount as amount
  from {{ ref('erc20_ethereum_transfers') }}
),

received_transfers as (
  select
    to as wallet_address,
    token_address,
    block_time,
    amount
  from {{ ref('erc20_ethereum_transfers') }}
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