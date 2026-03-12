select *
from {{ source('university', 'raw_payments') }}

