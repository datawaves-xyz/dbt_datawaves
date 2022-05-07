select 
  *
from {{ ref('common_ERC721_evt_Approval') }}
where owner is not null
and approved is not null
and tokenid is not null
