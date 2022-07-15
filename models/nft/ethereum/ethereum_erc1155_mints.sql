with transactions as (
  select *
  from {{ source('ethereum', 'transactions')}}
),

prices_usd as (
  select *
  from {{ source('ethereum', 'prices') }} 
),

erc1155_token_transfer_single as (
  select *
  from {{ source('ethereum_common', 'erc_1155_evt_transfer_single') }}
),

erc1155_mint_tx as (
  select
    a.hash as tx_hash,
    b.contract_address as nft_contract_address,
    b.id as nft_token_id,
    b.evt_block_time,
    b.to as minter,
    a.value,
    b.value as quantity
  from transactions as a
  join erc1155_token_transfer_single as b
    on a.hash = b.evt_tx_hash
      and b.from = '0x0000000000000000000000000000000000000000'
),

erc1155_mint as (
  select
    x.tx_hash,
    x.nft_contract_address,
    x.nft_token_id,
    x.quantity,
    x.evt_block_time,
    x.minter,
    y.avg_value / y.num_of_items / power(10, 18) as eth_mint_price
  from erc1155_mint_tx as x
  left join (
    select
      tx_hash,
      avg(value) as avg_value,
      count(distinct nft_token_id) as num_of_items
    from erc1155_mint_tx
    group by tx_hash
  ) as y
   on x.tx_hash = y.tx_hash
)

select
  u.tx_hash,
  u.nft_contract_address,
  u.nft_token_id,
  u.quantity,
  u.evt_block_time,
  u.minter,
  u.eth_mint_price,
  u.eth_mint_price * p.price as usd_mint_price
from erc1155_mint as u
left join prices_usd as p
  on p.minute = {{ dbt_utils.date_trunc('minute', 'u.evt_block_time') }}
    and p.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'