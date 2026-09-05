-- PostgreSQL Data Warehouse Schema for E-Commerce Pipeline
-- Database: ecommerce_dw

-- Drop tables if exists (Order matters due to Foreign Key constraints)
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

-- 1. CUSTOMER DIMENSION TABLE
CREATE TABLE dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    email VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    signup_date DATE
);

-- 2. PRODUCT DIMENSION TABLE
CREATE TABLE dim_product (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0)
);

-- 3. DATE DIMENSION TABLE
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY, -- Formatted as YYYYMMDD (e.g., 20260901)
    full_date DATE NOT NULL,
    day INT NOT NULL CHECK (day BETWEEN 1 AND 31),
    month INT NOT NULL CHECK (month BETWEEN 1 AND 12),
    quarter INT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    year INT NOT NULL,
    day_of_week VARCHAR(15)
);

-- 4. SALES FACT TABLE
CREATE TABLE fact_sales (
    order_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL REFERENCES dim_customer(customer_id),
    product_id VARCHAR(50) NOT NULL REFERENCES dim_product(product_id),
    date_id INT NOT NULL REFERENCES dim_date(date_id),
    order_date TIMESTAMP NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    total_amount NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
    order_status VARCHAR(50) NOT NULL DEFAULT 'Pending'
);

-- CREATE INDEXES FOR FAST QUERY PERFORMANCE ON ANALYTICAL JOINS
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_date ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_status ON fact_sales(order_status);
CREATE INDEX idx_dim_customer_country ON dim_customer(country);
CREATE INDEX idx_dim_product_category ON dim_product(category);
