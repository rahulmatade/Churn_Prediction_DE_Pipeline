select
    order_id,
    order_item_id,
    product_id,
    product_price,
    -- Assume each row = 1 unit (no quantity column)
    1 as quantity,
    product_price * 1 as item_total
    from {{ source('demo_raw_data', 'order_items') }}
    where order_id is not null
      and product_id is not null
      and product_price > 0


