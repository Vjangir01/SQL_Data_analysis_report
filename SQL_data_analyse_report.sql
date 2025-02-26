/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouseAnalytics' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouseAnalytics')
BEGIN
    ALTER DATABASE DataWarehouseAnalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouseAnalytics;
END;
GO

-- Create the 'DataWarehouseAnalytics' database
CREATE DATABASE DataWarehouseAnalytics;
GO

USE DataWarehouseAnalytics;
GO

-- Create Schemas

CREATE SCHEMA gold;
GO

CREATE TABLE gold.dim_customers(
	customer_key int,
	customer_id int,
	customer_number nvarchar(50),
	first_name nvarchar(50),
	last_name nvarchar(50),
	country nvarchar(50),
	marital_status nvarchar(50),
	gender nvarchar(50),
	birthdate date,
	create_date date
);
GO

CREATE TABLE gold.dim_products(
	product_key int ,
	product_id int ,
	product_number nvarchar(50) ,
	product_name nvarchar(50) ,
	category_id nvarchar(50) ,
	category nvarchar(50) ,
	subcategory nvarchar(50) ,
	maintenance nvarchar(50) ,
	cost int,
	product_line nvarchar(50),
	start_date date 
);
GO

CREATE TABLE gold.fact_sales(
	order_number nvarchar(50),
	product_key int,
	customer_key int,
	order_date date,
	shipping_date date,
	due_date date,
	sales_amount int,
	quantity tinyint,
	price int 
);
GO

TRUNCATE TABLE gold.dim_customers;
GO

BULK INSERT gold.dim_customers
FROM 'D:\Data_Set\sql-data-analytics-project-main\sql-data-analytics-project-main\datasets\csv-files\gold.dim_customers.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.dim_products;
GO

BULK INSERT gold.dim_products
FROM 'D:\Data_Set\sql-data-analytics-project-main\sql-data-analytics-project-main\datasets\csv-files\gold.dim_products.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.fact_sales;
GO

BULK INSERT gold.fact_sales
FROM 'D:\Data_Set\sql-data-analytics-project-main\sql-data-analytics-project-main\datasets\csv-files\gold.fact_sales.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO



select year(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales 
where order_date is not null 
group by year(order_date)
order by  year(order_date);

select month(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales 
where order_date is not null 
group by month(order_date)
order by  month(order_date);


select
DATETRUNC(year,create_date) as create_year,
count(customer_key) as total_customer
from gold.dim_customers
group by datetrunc(year,create_date)
order by DATETRUNC(year,create_date);


--Cumalative Analysis
select 
order_date,
total_sales,
sum(total_sales) over ( order by order_date) as running_total_status,
avg(avg_price) over ( order by order_date) as moving_avg_price
--window function
from 
(
select 
datetrunc(YEAR, order_date) as order_date,
sum(sales_amount ) as total_sales,
avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by datetrunc(YEAR, order_date)
)t;

-- Performance Analysis
-- Analysis the 
--current [measure] - target [measure]

with yearly_product_sales as(
select 
year(f.order_date) as order_year ,
p.product_name,
sum(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_products p 
on f.product_key = p.product_key
where f.order_date is not null 
group by 
year(f.order_date),
p.product_name
) select 
order_year,
product_name,
current_sales,
avg(current_sales) over(partition by product_name) avg_sales,
current_sales - avg(current_sales) over(partition by product_name) as diff_avg,
case when current_sales - avg(current_sales) over(partition by product_name) > 0 then 'Above Avg'
     when current_sales - avg(current_sales) over(partition by product_name) < 0 then 'Below Avg'
	 else 'Avg'
end avg_change,
--year-over-year-analysis
lag(current_sales) over (partition by product_name order by order_year) py_sales,
current_sales -lag(current_sales) over (partition by product_name order by order_year) as diff_py_sales,
case when current_sales - lag(current_sales) over (partition by product_name order by order_year) > 0 then 'Increase'
     when current_sales - lag(current_sales) over (partition by product_name order by order_year) < 0 then 'Decrease'
	 else 'No Change'
end py_change
from yearly_product_sales
order by product_name, order_year;

--Part To Whole Proportional Analysis

--Advanced Analytics Project

--Q. find catrgories contribues the most to overall sales
with category_sales as (
select 
category,
sum(sales_amount) total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by category )
select 
category,
total_sales,
sum(total_sales) over () overall_sales,
CONCAT(round((cast(total_sales as float)/sum(total_sales) over ())*100,2),'%') as perct_of_total
from category_sales
order by total_sales desc;

-- Advanced Analytics Project 
-- Data Segmentation
--/*Q.Segment product in cost range and 
--     count how many product fall into each segement */

with CTC as(
select 
product_key,
product_name,
cost,
case when cost < 100 then 'Below 100'
     when cost between  100 and 500  then '100-500'
	 when cost between 500 and 1000 then '500-1000'
	 else 'Above 1000'
end cost_range
from gold.dim_products)
select
cost_range,
count(product_key) as total_products
from CTC
group by cost_range
order by total_products desc;



-- SQL Task

with ctc as (
select 
c.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date) as first_order,
max(order_date) as last_order,
datediff(month,min(order_date),max(order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.customer_key)

select 
customer_segment,
count(customer_key) as total_customers
from (
select
customer_key,
case when lifespan >= 12 and total_spending > 5000 then 'VIP'
     when lifespan >=12 and total_spending  <= 5000 then 'Regular'
	 else 'New'
end customer_segment 
from ctc)t
group by customer_segment
order by total_customers desc;

--customer_report

create view gold.report_customers AS
with cte as(
select 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name, ' ', c.last_name) as customer_name,
DATEDIFF(YEAR,c.birthdate,GETDATE()) as age
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
where order_date is not null )

, customer_aggregation as(
select 
customer_key,
customer_number,
customer_name,
age,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct product_key) as total_products,
max(order_date) as last_order_date,
datediff(month,min(order_date),max(order_date)) as lifespan
from cte
group by 
    customer_key,
	customer_number,
	customer_name,
	age)
select 
customer_key,
customer_name,
customer_number,
age,
case when age < 20 then 'Under 20'
     when age between 20 and 20 then '20-29'
	 when age between 30 and 39 then '30-39'
	 when age between 40 and 49 then '40-49'
	 else '50 and above'
end as Age_group,
case when lifespan >= 12 and total_sales > 5000 then 'VIP'
     when lifespan >=12 and total_sales  <= 5000 then 'Regular'
	 else 'New'
end as customer_segment,
total_orders,
total_sales,
total_quantity,
total_products,
last_order_date,
datediff(month,last_order_date,getdate()) as recency,
lifespan,
-- Compuate avg order value(AVO)
case when total_sales = 0 then 0
     else total_sales / total_orders
end as avg_order_value,
-- compuate avg monthly spend
case when lifespan = 0 then total_sales
     else total_sales/lifespan
end as avg_monthly_spend
from customer_aggregation

select * from gold.report_customers;