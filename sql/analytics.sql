-- E-Commerce Business Analytics SQL Queries
-- Data Warehouse: PostgreSQL (Star Schema)

-- 1. TOTAL REVENUE & ORDER COUNT
SELECT 
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_units_sold,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS average_order_value
FROM fact_sales
WHERE order_status != 'Cancelled';

-- 2. MONTHLY REVENUE TREND
SELECT 
    d.year,
    d.month,
    COUNT(f.order_id) AS order_count,
    SUM(f.total_amount) AS monthly_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.order_status != 'Cancelled'
GROUP BY d.year, d.month
ORDER BY d.year DESC, d.month DESC;

-- 3. DAILY REVENUE TREND
SELECT 
    d.full_date,
    COUNT(f.order_id) AS daily_orders,
    SUM(f.total_amount) AS daily_revenue
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
WHERE f.order_status != 'Cancelled'
GROUP BY d.full_date
ORDER BY d.full_date DESC;

-- 4. TOP 10 PRODUCTS BY SALES REVENUE
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.total_amount) AS total_product_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
WHERE f.order_status != 'Cancelled'
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_product_revenue DESC
LIMIT 10;

-- 5. REVENUE BY PRODUCT CATEGORY
SELECT 
    p.category,
    COUNT(f.order_id) AS total_orders,
    SUM(f.total_amount) AS category_revenue,
    ROUND(SUM(f.total_amount) * 100.0 / SUM(SUM(f.total_amount)) OVER(), 2) AS revenue_percentage
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
WHERE f.order_status != 'Cancelled'
GROUP BY p.category
ORDER BY category_revenue DESC;

-- 6. REVENUE BY COUNTRY
SELECT 
    c.country,
    COUNT(DISTINCT c.customer_id) AS active_customers,
    COUNT(f.order_id) AS total_orders,
    SUM(f.total_amount) AS country_revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE f.order_status != 'Cancelled'
GROUP BY c.country
ORDER BY country_revenue DESC;

-- 7. REPEAT CUSTOMERS ANALYSIS (Customers with > 1 completed order)
SELECT 
    c.customer_id,
    c.name,
    c.email,
    c.country,
    COUNT(f.order_id) AS completed_orders,
    SUM(f.total_amount) AS customer_lifetime_value
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE f.order_status != 'Cancelled'
GROUP BY c.customer_id, c.name, c.email, c.country
HAVING COUNT(f.order_id) > 1
ORDER BY customer_lifetime_value DESC;

-- 8. ORDER STATUS BREAKDOWN (Completed vs Cancelled vs Pending)
SELECT 
    order_status,
    COUNT(order_id) AS status_count,
    ROUND(COUNT(order_id) * 100.0 / SUM(COUNT(order_id)) OVER(), 2) AS status_percentage,
    SUM(total_amount) AS status_total_value
FROM fact_sales
GROUP BY order_status
ORDER BY status_count DESC;

-- 9. AVERAGE ORDER VALUE (AOV) BY COUNTRY
SELECT 
    c.country,
    COUNT(f.order_id) AS total_orders,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE f.order_status != 'Cancelled'
GROUP BY c.country
ORDER BY avg_order_value DESC;
