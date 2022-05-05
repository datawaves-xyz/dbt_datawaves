with nft_trades as (
  select *
  from {{ ref('nft_trades') }}
),

contracts as (
    select distinct address
    from {{ ref('stg_contracts') }}
),

trader as (
    select 
        address, 
        sum(amount) as profit
    from (
        select seller as address, currency_amount as amount
        from ethereum_nft.nft_trades
        where currency_symbol in ('ETH','WETH')

        union all
        select buyer as address, -currency_amount as amount
        from ethereum_nft.nft_trades
        where currency_symbol in ('ETH','WETH')
    )
    group by 1
),

smart_trader as (
    select trader.address, trader.profit
    from trader
    left anti join contracts 
    on stat.address = contracts.address
    order by trader.profit desc
    limit 100
)


select 
    address, 
    'Smart NFT Trader' as label,
    'Smart Money' as label_type
from smart_trader
