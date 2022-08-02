with transactions as (
  select *
  from {{ source('ethereum', 'transactions')}}
),

nft_transfers as (
  select 'erc1155' as erc_standard, *
  from {{ ref('erc1155_ethereum_transfers') }}

  union all

  select 'erc721' as erc_standard, *
  from {{ ref('erc721_ethereum_transfers') }}
),

prices_usd as (
  select *
  from {{ source('ethereum', 'prices') }} 
),

mint_tx as (
  select
    a.hash as tx_hash,
    a.value,
    b.token_address as nft_contract_address,
    b.token_id as nft_token_id,
    b.block_time,
    b.to as minter,    
    b.amount as quantity,
    b.erc_standard
  from transactions as a
  join nft_transfers as b
    on a.hash = b.tx_hash and b.`from` = '0x0000000000000000000000000000000000000000'
),

mint as (
  select
    x.erc_standard,
    x.tx_hash,
    x.nft_contract_address,
    x.nft_token_id,
    x.quantity,
    x.block_time,
    x.minter,
    ({{ datawaves_utils.displayed_amount('y.avg_value', 18) }} / num_of_items) as eth_mint_price
  from mint_tx as x
  left join (
    select
      tx_hash,
      avg(value) as avg_value,
      count(distinct nft_token_id) as num_of_items
    from mint_tx
    group by tx_hash
  ) as y
   on x.tx_hash = y.tx_hash
)

select
  'Ethereum' as blockchain,
  u.erc_standard,
  u.tx_hash,
  u.nft_contract_address,
  u.nft_token_id,
  u.quantity,
  u.block_time,
  u.minter,
  u.eth_mint_price,
  u.eth_mint_price * p.price as usd_mint_price
from mint as u
left join prices_usd as p
  on p.minute = {{ dbt_utils.date_trunc('minute', 'u.block_time') }}
    and p.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'