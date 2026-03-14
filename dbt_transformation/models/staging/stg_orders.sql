select
    order_id,
    customer_id,
    order_purchased_at,
    order_approved_at,
    order_delivered_at,
    status,
    -- Use approved_at as primary date, fallback to purchased_at
    coalesce(order_approved_at, order_purchased_at) as order_date
    from {{ source('demo_raw_data', 'orders') }}
    where order_id is not null
      and customer_id is not null
      -- Only include orders with valid dates
      and (order_approved_at is not null or order_purchased_at is not null)



