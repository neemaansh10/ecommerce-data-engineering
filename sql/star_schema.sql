-- Star Schema Model Documentation & Convenience Analytical Views
-- Architecture:
-- 
--                 dim_customer (Dimension: Who bought?)
--                       |
--                       | 1:N
-- dim_product ---- fact_sales ---- dim_date (Dimension: When bought?)
-- (Dimension:      (Fact Table: What was sold? Total Amount, Quantity)
--  What bought?)

-- CREATE CONVENIENCE VIEW FOR REPORTING & DASHBOARDS
CREATE OR REPLACE VIEW vw_sales_summary AS
SELECT 
    f.order_id,
    f.order_date,
    d.full_date,
    d.year,
    d.month,
    d.quarter,
    c.customer_id,
    c.name AS customer_name,
    c.city,
    c.country,
    p.product_id,
    p.product_name,
    p.category,
    f.quantity,
    f.unit_price,
    f.total_amount,
    f.order_status
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_date d ON f.date_id = d.date_id;
