with nft_trades as (
  select *
  from {{ ref('nft_trades') }}
),

contracts as (
  select distinct address
  from {{ ref('stg_contracts') }}
),

erc721_token_transfers as (
  select *
  from {{ ref('ERC721_evt_Transfer') }}
),

floor_price_info as (
  select
    nft_contract_address,
    floor_price
  from (
    select
      dt,
      nft_contract_address,
      floor_price,
      row_number()over(partition by nft_contract_address order by dt desc) as rank
    from (
      select
        dt,
        nft_contract_address,
        percentile(eth_amount, 0.05) as floor_price
      from nft_trades
      group by dt, nft_contract_address
    )
    where rank = 1
  )
),

holder_info as (
  select distinct
    nft_contract_address,
    nft_token_id,
    to_address as address,
    eth_amount
  from (
    select
      nft_contract_address,
      nft_token_id,
      to_address,
      eth_amount,
      row_number()over(partition by nft_contract_address, nft_token_id order by block_time desc) as rank
    from (
      select
        nft_contract_address,
        nft_token_id,
        buyer as to_address,
        eth_amount,
        block_time
      from nft_trades
      union
      select
        contract_address as nft_contract_address,
        tokenid as nft_token_id,
        to as to_address,
        0 as eth_amount,
        evt_block_time as block_time
      from erc721_token_transfers
      where from = '0x0000000000000000000000000000000000000000'
    )
  )
  where rank = 1
),

smart_holder as (
  select
    address,
    sum(floor_price - eth_amount) as estimated_profit
  from (
    select
      holder_info.address,
      holder_info.nft_contract_address,
      holder_info.nft_token_id,
      holder_info.eth_amount,
      floor_price_info.floor_price
    from holder_info
    left join floor_price_info
      on holder_info.nft_contract_address = floor_price_info.nft_contract_address
    left anti join contracts
      on holder_info.address = contracts.address
  )
  group by address
  order by estimated_profit desc
  limit 100
)

select
  address,
  'Smart NFT Holder' as label,
  'Smart Money' as label_type
from smart_holder
