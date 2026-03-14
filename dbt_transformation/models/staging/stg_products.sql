select
        product_id,
        name,
        lower(category) as category,
        price as product_price,
        coalesce(rating, 0) as rating,
        case 
            when availability = true then 1 
            else 0 
        end as is_available
    from {{ source('demo_raw_data', 'products') }}
    where product_id is not null


