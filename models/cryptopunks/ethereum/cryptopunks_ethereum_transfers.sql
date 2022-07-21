select
  contract_address as nft_contract_address,
  punk_index as nft_token_id,
  to as to_address,
  evt_block_time as block_time
from {{ source('ethereum_cryptopunks', 'crypto_punks_market_evt_punk_transfer') }}
union distinct
select
  contract_address as nft_contract_address,
  punk_index as nft_token_id,
  to_address,
  evt_block_time as block_time
from {{ source('ethereum_cryptopunks', 'crypto_punks_market_evt_punk_bought') }}
union distinct
select
  contract_address as nft_contract_address,
  punk_index as nft_token_id,
  to as to_address,
  evt_block_time as block_time
from {{ source('ethereum_cryptopunks', 'crypto_punks_market_evt_assign') }}