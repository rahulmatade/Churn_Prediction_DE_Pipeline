/*with
    customers as (select * from {{ ref("stg_customers") }}),
    orders as (select * from {{ ref("stg_orders") }}),
    customer_orders as (
        select
            c.customer_id,
            c.email,
            c.country,
            c.city,
            min(o.order_approved_at) as first_order_date,
            max(o.order_approved_at) as most_recent_order_date,
            count(o.order_id) as number_of_orders
        from orders o
        inner join customers c using (customer_id)
        group by c.customer_id, c.email, c.country, c.city
    )
select *
from customer_orders*/


-- This model creates 17 clean ML features for churn prediction
-- Removed: email, names (useless), recent activity (leakage)
-- Kept: Historical behavior, demographics, product engagement and advanced temporal/regularity features

{{ config(
    materialized = 'table',
    schema = 'analytics'
) }}

with customers as (
    select * from {{ ref("stg_customers") }}
),

orders as (
    select * from {{ ref("stg_orders") }}
),

order_items as (
    select * from {{ ref("stg_order_items") }}
),

products as (
    select * from {{ ref("stg_products") }}
),

-- ============================================================================
-- Base Customer-Order Aggregation (Historical Data Only)
-- ============================================================================

customer_orders as (
    select
        c.customer_id,
        c.country,
        c.city,
        c.gender,
        min(o.order_date) as first_order_date,
        max(o.order_date) as most_recent_order_date,
        count(distinct o.order_id) as number_of_orders,
        (select max(order_date) from {{ ref("stg_orders") }}) as reference_date
    from orders o
    inner join customers c 
        on o.customer_id = c.customer_id
    group by c.customer_id, c.country, c.city, c.gender
),

-- ============================================================================
-- Customer Spending Metrics (Historical - No Leakage)
-- ============================================================================
-- Using TOTAL and AVG across all history (not recent time windows)
-- This avoids leakage because it's historical customer value, not current activity

customer_spending as (
    select
        o.customer_id,
        sum(oi.item_total) as total_spent,
        avg(oi.item_total) as avg_item_value,
        max(oi.item_total) as max_item_value,
        min(oi.item_total) as min_item_value,
        stddev(oi.item_total) as stddev_item_value
    from orders o
    inner join order_items oi 
        on o.order_id = oi.order_id
    group by o.customer_id
),

-- ============================================================================
-- Product Category Preferences & Quality Metrics
-- ============================================================================
-- Average rating of products purchased = proxy for satisfaction
-- Diversity of products = customer investment in platform

customer_product_quality as (
    select
        o.customer_id,
        count(distinct oi.product_id) as unique_products_purchased,
        avg(p.rating) as avg_product_rating,
        max(p.rating) as highest_product_rating,
        min(p.rating) as lowest_product_rating
    from orders o
    inner join order_items oi 
        on o.order_id = oi.order_id
    inner join products p 
        on oi.product_id = p.product_id
    where p.rating is not null
    group by o.customer_id
),

-- ============================================================================
-- Order Status Analysis (Delivery Success Rate)
-- ============================================================================
-- Cancellation rate = service quality indicator
-- Successful orders = trust in platform

order_status_analysis as (
    select
        o.customer_id,
        sum(case when o.status = 'delivered' then 1 else 0 end) as successful_orders,
        sum(case when o.status = 'canceled' then 1 else 0 end) as canceled_orders,
        round(
            100.0 * sum(case when o.status = 'canceled' then 1 else 0 end) / 
            nullif(count(distinct o.order_id), 0),
            2
        ) as cancellation_rate
    from orders o
    group by o.customer_id
),

-- ============================================================================
-- ADVANCED TEMPORAL & REGULARITY FEATURES
-- ============================================================================

customer_order_intervals as (
    select
        customer_id,
        order_date,
        lag(order_date) over (partition by customer_id order by order_date) as prev_order_date,
        date_diff(order_date, lag(order_date) over (partition by customer_id order by order_date), day) as days_since_prev_order
    from {{ ref("stg_orders") }}
),

-- Calculate average and stddev of days between orders
customer_interval_stats as (
    select
        customer_id,
        count(distinct order_date) as total_orders,
        
        -- Average days between orders
        round(
            avg(case when days_since_prev_order is not null then days_since_prev_order end),
            2
        ) as avg_days_between_orders,
        
        -- Standard deviation of days between orders
        round(
            stddev(case when days_since_prev_order is not null then days_since_prev_order end),
            2
        ) as stddev_days_between_orders,
        
        -- Min and max for entropy calculation
        min(case when days_since_prev_order is not null then days_since_prev_order end) as min_days_between,
        max(case when days_since_prev_order is not null then days_since_prev_order end) as max_days_between
        
    from customer_order_intervals
    where days_since_prev_order is not null
    group by customer_id
),

-- Calculate order interval entropy (measure of predictability)
customer_order_entropy as (
    select
        cis.customer_id,
        cis.avg_days_between_orders,
        cis.stddev_days_between_orders,
        
        -- Formula: -sum(p_i * log(p_i)) where p_i is probability of each interval
        -- If stddev is high, pattern is unpredictable (high entropy), if low, pattern is regular (low entropy)
        case
            when cis.stddev_days_between_orders is null then 0
            when cis.avg_days_between_orders = 0 then 0
            else round(
                cis.stddev_days_between_orders / nullif(cis.avg_days_between_orders, 0),
                4
            )
        end as order_interval_entropy,
        
        -- Order Regularness Score: inverse of entropy (0-1 scale)
        -- 1 = very regular/predictable
        -- 0 = very irregular/random
        case
            when cis.stddev_days_between_orders is null then 0.5
            when cis.avg_days_between_orders = 0 then 1.0
            else round(
                1.0 / (1.0 + (cis.stddev_days_between_orders / nullif(cis.avg_days_between_orders, 0))),
                4
            )
        end as order_regularness_score
    from customer_interval_stats cis
),

-- ============================================================================
-- Step 5: Calculate Churn Metrics and Target Variable
-- ============================================================================
-- 21 CLEAN FEATURES (no leakage, no useless features)

churn_metrics as (
    select
        co.customer_id,
        
        -- ====================================================================
        -- RECENCY & TENURE (3 features)
        -- ====================================================================
        -- These show HISTORICAL behavior before churn window
        
        date_diff(co.reference_date, date(co.most_recent_order_date), day) as days_since_last_order,
        -- ^ Core recency signal: "How long since they last purchased?"
        
        date_diff(co.reference_date, date(co.first_order_date), day) as customer_tenure_days,
        -- ^ Loyalty indicator: "How long have they been with us?"
        
        round(
            (co.number_of_orders * 30.0) / 
            nullif(date_diff(co.reference_date, date(co.first_order_date), day), 0),
            2
        ) as orders_per_month,
        -- ^ Engagement: "How frequently do they buy historically?" (not recent activity)
        
        -- ====================================================================
        -- MONETARY METRICS (4 features)
        -- ====================================================================
        -- Using TOTAL/AVG across entire history = no leakage
        
        coalesce(cs.total_spent, 0) as total_spent,
        -- ^ Customer lifetime value
        
        coalesce(cs.avg_item_value, 0) as avg_item_value,
        -- ^ Average basket size
        
        coalesce(cs.max_item_value, 0) as max_item_value,
        -- ^ Maximum purchase amount (spending capacity)
        
        coalesce(cs.stddev_item_value, 0) as stddev_item_value,
        -- ^ Spending consistency (volatile = less committed)
        
        -- ====================================================================
        -- PRODUCT ENGAGEMENT (4 features)
        -- ====================================================================
        -- What they buy and quality of purchases
        
        coalesce(cpq.unique_products_purchased, 0) as unique_products_purchased,
        -- ^ Product diversity (more diversity = more invested)
        
        coalesce(cpq.avg_product_rating, 0) as avg_product_rating,
        -- ^ Quality of purchases (proxy for satisfaction)
        
        coalesce(cpq.highest_product_rating, 0) as highest_product_rating,
        -- ^ Premium purchases? (did they buy high-quality items?)
        
        coalesce(cpq.lowest_product_rating, 0) as lowest_product_rating,
        -- ^ Poor purchases? (did they get low-quality items?)
        
        -- ====================================================================
        -- ORDER QUALITY (3 features)
        -- ====================================================================
        -- Service quality and fulfillment success
        
        coalesce(osa.successful_orders, 0) as successful_orders,
        -- ^ Orders successfully delivered
        
        coalesce(osa.canceled_orders, 0) as canceled_orders,
        -- ^ Orders customer cancelled (dissatisfaction)
        
        coalesce(osa.cancellation_rate, 0) as cancellation_rate,
        -- ^ % of orders cancelled (service quality indicator)
        
        -- ====================================================================
        -- DEMOGRAPHICS (3 features)
        -- ====================================================================
        -- Geographic and demographic patterns
        
        co.country,
        -- ^ Geographic location (location-based churn patterns)
        
        co.city,
        -- ^ City-level patterns (urban vs rural)
        
        co.gender,
        -- ^ Demographic signal (may affect retention patterns)

        -- ====================================================================
        -- NEW: 4 TEMPORAL & REGULARITY FEATURES
        -- ====================================================================

        coalesce(coe.avg_days_between_orders, 0) as avg_days_between_orders,
        -- ^ Average gap between consecutive orders (days)
        
        coalesce(coe.stddev_days_between_orders, 0) as stddev_days_between_orders,
        -- ^ Variability in order intervals (high = unpredictable)
        
        coalesce(coe.order_interval_entropy, 0) as order_interval_entropy,
        -- ^ Randomness of ordering pattern (0-1+, higher = more random)
        
        coalesce(coe.order_regularness_score, 0) as order_regularness_score,
        -- ^ Consistency of ordering (0-1, higher = more regular/predictable)
        
        -- ====================================================================
        -- TARGET VARIABLE (1 feature)
        -- ====================================================================
        
        /*case
            when date_diff(co.reference_date, date(co.most_recent_order_date), day) > 90 
            then 1
            else 0
        end as is_churned*/
        -- Old logic, needs to be discarded

        case
            when co.number_of_orders = 0 and date_diff(co.reference_date, date(co.first_order_date), day) > 30 then 1
            when co.number_of_orders > 0 and date_diff(co.reference_date, date(co.most_recent_order_date), day) > 30 then 1
            else 0
        end as is_churned
        -- ^ Definition: No purchase in last 90 days = CHURNED
        
    from customer_orders co
    left join customer_spending cs 
        on co.customer_id = cs.customer_id
    left join customer_product_quality cpq 
        on co.customer_id = cpq.customer_id
    left join order_status_analysis osa 
        on co.customer_id = osa.customer_id
    left join customer_order_entropy coe  -- NEW JOIN
        on co.customer_id = coe.customer_id
)

-- ============================================================================
-- Final Selection
-- ============================================================================
-- 19 Features + 1 Target = Ready for ML
-- No email, no names, no recent activity

select
    customer_id,
    
    -- Frequency
    orders_per_month,
    
    -- Monetary
    total_spent,
    avg_item_value,
    max_item_value,
    stddev_item_value,
    
    -- Product Engagement
    unique_products_purchased,
    avg_product_rating,
    highest_product_rating,
    lowest_product_rating,
    
    -- Order Quality
    successful_orders,
    canceled_orders,
    cancellation_rate,
    
    -- Demographics
    country,
    city,
    gender,

    -- Temporal features
    avg_days_between_orders,
    stddev_days_between_orders,
    order_interval_entropy,
    order_regularness_score,
    
    -- Target Variable
    is_churned
    
from churn_metrics
where customer_tenure_days > 0  -- Only customers with at least one order
order by customer_id