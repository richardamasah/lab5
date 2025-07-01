


##  ATHENA BUSINESS INSIGHT QUERIES



### 1.  **Top 10 Most Sold Products**

sql
SELECT 
  p.product_name, 
  COUNT(oi.product_id) AS total_sales
FROM lakehouse_dwh.order_items oi
JOIN lakehouse_dwh.products p ON oi.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_sales DESC
LIMIT 10;


### 2.  **Daily Revenue Trend**

sql
SELECT 
  o.date, 
  ROUND(SUM(o.total_amount), 2) AS daily_revenue
FROM lakehouse_dwh.orders o
GROUP BY o.date
ORDER BY o.date ASC;


### 3.  **Most Active Customers (by Orders)**

sql
SELECT 
  user_id, 
  COUNT(order_id) AS total_orders
FROM lakehouse_dwh.orders
GROUP BY user_id
ORDER BY total_orders DESC
LIMIT 10;


### 4.  **Reorder Rate (Customer Retention Indicator)**

sql
SELECT 
  reordered, 
  COUNT(*) AS count
FROM lakehouse_dwh.order_items
GROUP BY reordered;




### 5.  **Total Orders and Items Per Day**

sql
SELECT 
  o.date, 
  COUNT(DISTINCT o.order_id) AS total_orders,
  COUNT(oi.id) AS total_items
FROM lakehouse_dwh.orders o
JOIN lakehouse_dwh.order_items oi ON o.order_id = oi.order_id
GROUP BY o.date
ORDER BY o.date;


### 6.  **Average Order Value Per Day**

sql
SELECT 
  date, 
  ROUND(AVG(total_amount), 2) AS avg_order_value
FROM lakehouse_dwh.orders
GROUP BY date
ORDER BY date;

### 7.  **Department-Level Product Sales**

sql
SELECT 
  p.department, 
  COUNT(oi.product_id) AS items_sold
FROM lakehouse_dwh.order_items oi
JOIN lakehouse_dwh.products p ON oi.product_id = p.product_id
GROUP BY p.department
ORDER BY items_sold DESC;


### 8.  **Days Since Prior Order Distribution**

sql
SELECT 
  days_since_prior_order, 
  COUNT(*) AS frequency
FROM lakehouse_dwh.order_items
GROUP BY days_since_prior_order
ORDER BY days_since_prior_order;

### 9.  **Invalid Product Records (if logged)**

sql
SELECT *
FROM lakehouse.rejected.products;




### 10.  **Total Revenue to Date**

sql
SELECT 
  ROUND(SUM(total_amount), 2) AS total_revenue
FROM lakehouse_dwh.orders;


