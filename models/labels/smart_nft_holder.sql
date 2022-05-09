with nft_trades as (
  select *
  from {{ ref('nft_trades') }}
),

contracts as (
  select distinct address
  from {{ ref('stg_contracts') }}
),

token_transfers as (
  select *
  from {{ ref('stg_token_transfers') }}
),

floor_price_info as (
  select 
    nft_contract_address,
    percentile(currency_amount,0.05) as floor_price
  from nft_trades
  where currency_symbol in ('ETH','WETH') and to_date(block_time) = date_sub(current_date(), 1)
  group by nft_contract_address
),

holder_info as (
  select 
    distinct
    nft_contract_address,
    nft_token_id,
    to_address as address,
    currency_amount
  from (
    select 
      nft_contract_address,
      nft_token_id,
      to_address,
      currency_amount,
      row_number()over(partition by nft_contract_address, nft_token_id order by block_time desc) as rank
    from (
      select 
        nft_contract_address,
        nft_token_id,
        buyer as to_address,
        currency_amount,
        block_time
      from nft_trades
            
      union

      select 
        token_address as nft_contract_address,
        cast(value as string) as nft_token_id,
        to_address,
        0 as currency_amount,
        block_timestamp as block_time
      from token_transfers
      where from_address = '0x0000000000000000000000000000000000000000'
    )
  )
  where rank = 1
),

smart_holder as (
  select 
    address,
    sum(floor_price-currency_amount) as estimated_profit
  from (
    select 
      holder_info.address,
      holder_info.nft_contract_address,
      holder_info.nft_token_id,
      holder_info.currency_amount,
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