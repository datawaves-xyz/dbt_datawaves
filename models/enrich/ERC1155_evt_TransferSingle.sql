select 
  *
from {{ ref('common_ERC1155_evt_TransferSingle') }}
