select 
  *
from {{ ref('common_ERC20_evt_Transfer') }}
where from is not null
and to is not null
and value is not null
