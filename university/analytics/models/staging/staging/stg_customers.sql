select *
from {{ source('university', 'raw_customers') }}

