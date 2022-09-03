{{config(alias='reverse_registrars')}}

select
  *
from {{ ref('ens_ethereum_reverse_registrars') }}