select *
from {{source('ethereum_ens', 'ethregistrar_controller_evt_name_registered')}}
union
select *
from {{source('ethereum_ens', 'ethregistrar_controller_2_evt_name_registered')}}
union
select *
from {{source('ethereum_ens', 'ethregistrar_controller_3_evt_name_registered')}}