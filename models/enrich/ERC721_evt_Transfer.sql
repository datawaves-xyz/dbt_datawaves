select 
  *
from {{ ref('common_ERC721_evt_Transfer') }}
where from is not null
and to is not null
and tokenid is not null
