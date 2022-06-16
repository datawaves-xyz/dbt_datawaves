with nft_trades as (
  select *
  from {{ ref('nft_trades') }}
),

contracts as (
  select distinct address
  from {{ source('ethereum', 'contracts') }}
),

stat as (
  select *
  from (
    select
      buyer as address,
      sum(usd_amount) as amount
    from (
      select distinct
        buyer,
        usd_amount,
        nft_token_id,
        nft_contract_address
      from (
        select
          buyer,
          nft_contract_address,
          nft_token_id,
          usd_amount,
          row_number() over(partition by nft_contract_address, nft_token_id order by block_time desc) as rank
        from nft_trades
      )
      where rank = 1
    )
    group by address
  )
  where amount > 1000000
  order by amount desc
)

select
  stat.address,
  'NFT Millionaire' as label,
  'NFT Whale' as label_type
from stat
left anti join contracts
  on stat.address = contracts.address
