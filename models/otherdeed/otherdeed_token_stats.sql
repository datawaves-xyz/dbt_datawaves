select
  x.nft_token_id,
  x.latest_eth_amount,
  y.*

from
  (select distinct
    nft_token_id,
    eth_amount as latest_eth_amount
    from (
      select distinct
        nft_token_id,
        eth_amount,
        row_number() over (partition by nft_contract_address, nft_token_id order by block_time desc) as rank_by_time
      from ethereum_nft.nft_trades
      where nft_contract_address = '0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258'
    )
    where rank_by_time = 1
  ) x

left join ethereum_nft_metadata.otherdeed y
  on x.nft_token_id = y.token_id
