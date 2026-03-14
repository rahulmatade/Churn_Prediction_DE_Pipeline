select customer_id, 
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        lower(email) as email,
        gender, 
        city, 
        country
from {{ source("demo_raw_data", "customers")}}
where customer_id is not null