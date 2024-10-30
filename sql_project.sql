SELECT TOP 10 *
FROM DBO.e_commerce_data;

--PART 1: Analyze the data by finding the answers to the questions below:
--1. Find the top 3 customers who have the maximum count of orders.

SELECT TOP 3 Cust_ID, COUNT(*) as order_count 
FROM dbo.e_commerce_data
GROUP BY Cust_ID
ORDER BY order_count DESC;

--2. Find the customer whose order took the maximum time to get shipping.
SELECT TOP 1 Cust_ID, Order_Date, Ship_Date, DATEDIFF(DAY, Order_Date, Ship_Date) as ship_time
FROM dbo.e_commerce_data
ORDER BY ship_time DESC;

--3. Count the total number of unique customers in January and how many of them came back again in the each one months of 2011.
SELECT COUNT(DISTINCT Cust_ID) as unique_cust_jan
FROM dbo.e_commerce_data
WHERE Order_Date >= '2011-01-01' AND Order_Date < '2011-02-01';

WITH jan_custs AS (
    SELECT DISTINCT Cust_ID
    FROM dbo.e_commerce_data
    WHERE Order_Date >= '2011-01-01' AND Order_Date < '2011-02-01'
)

SELECT 
    MONTH(Order_Date) AS months_of_2011,
    COUNT(DISTINCT e.Cust_ID) as returning_customers
FROM dbo.e_commerce_data as e
JOIN jan_custs as jc ON e.Cust_ID= jc.Cust_ID
WHERE Order_Date >= '2011-02-01' AND Order_Date < '2012-01-01'
GROUP BY MONTH(Order_Date)
ORDER BY months_of_2011;

--4. Write a query to return for each user the time elapsed between the first purchasing and the third purchasing, in ascending order by Customer ID.
WITH RankedPurchases AS (
    SELECT 
        Cust_ID,
        Order_Date,
        ROW_NUMBER() OVER (PARTITION BY Cust_ID ORDER BY Order_Date) AS purchase_rank
    FROM dbo.e_commerce_data 
)

SELECT 
    Cust_ID,
    DATEDIFF(day, 
             MIN(CASE WHEN purchase_rank = 1 THEN Order_Date END), 
             MIN(CASE WHEN purchase_rank = 3 THEN Order_Date END)) AS elapsed_time
FROM RankedPurchases
WHERE purchase_rank IN (1, 3)
GROUP BY Cust_ID
HAVING COUNT(*) = 2  
ORDER BY CAST(SUBSTRING(Cust_ID, PATINDEX('%[0-9]%', Cust_ID), LEN(Cust_ID)) AS INT) ASC;

--5. Write a query that returns customers who purchased both product 11 and product 14, as well as the ratio of these products to the total number of products purchased by the customer.
WITH CustomerPurchases AS (
    SELECT 
        Cust_ID,
        COUNT(CASE WHEN Prod_ID = 'Prod_11' THEN 1 END) AS product_11_count,
        COUNT(CASE WHEN Prod_ID = 'Prod_14' THEN 1 END) AS product_14_count,
        COUNT(*) AS total_product_count
    FROM dbo.e_commerce_data 
    GROUP BY Cust_ID
)

SELECT 
    Cust_ID, 
    (product_11_count + product_14_count) * 1.0 / total_product_count AS product_ratio
FROM CustomerPurchases
WHERE product_11_count > 0 AND product_14_count > 0
ORDER BY CAST(SUBSTRING(Cust_ID, PATINDEX('%[0-9]%', Cust_ID), LEN(Cust_ID)) AS INT) ASC;


--PART 2: Customer Segmentation 
--Categorize customers based on their frequency of visits. The following steps will guide you. If you want, you can track your own way.

--1. Create a “view” that keeps visit logs of customers on a monthly basis. (For each log, three field is kept: Cust_id, Year, Month)

CREATE VIEW MonthlyVisitLogs AS
SELECT 
    Cust_ID AS Cust_id,
    YEAR(Order_Date) AS Year,
    MONTH(Order_Date) AS Month
FROM dbo.e_commerce_data 
GROUP BY Cust_ID, YEAR(Order_Date), MONTH(Order_Date);

SELECT * 
FROM MonthlyVisitLogs

--2. Create a “view” that keeps the number of monthly visits by users. (Show separately all months from the beginning business)
CREATE VIEW MonthlyVisitCounts AS
SELECT 
    Cust_ID AS Cust_id,
    YEAR(Order_Date) AS Year,
    MONTH(Order_Date) AS Month,
    COUNT(*) AS visit_count
FROM dbo.e_commerce_data 
GROUP BY Cust_ID, YEAR(Order_Date), MONTH(Order_Date);

SELECT * 
FROM MonthlyVisitCounts
ORDER BY Year, Month, Cust_id;

--3. For each visit of customers, create the previous or next month of the visit as a separate column.
CREATE VIEW VisitWithAdjMonths AS
SELECT 
    Cust_ID AS Cust_id,
    Order_Date AS VisitDate,
    YEAR(Order_Date) AS Year,
    MONTH(Order_Date) AS Month,
    DATEADD(MONTH, -1, Order_Date) AS PreviousMonth,
    DATEADD(MONTH, 1, Order_Date) AS NextMonth
FROM dbo.e_commerce_data;

Select * 
FROM VisitWithAdjMonths;

--4. Calculate the monthly time gap between two consecutive visits by each customer.
CREATE VIEW MonthlyTimeGap AS
SELECT 
    Cust_ID AS Cust_id,
    Order_Date AS VisitDate,
    LAG(Order_Date) OVER (PARTITION BY Cust_ID ORDER BY Order_Date) AS PreviousVisitDate,
    DATEDIFF(MONTH, 
             LAG(Order_Date) OVER (PARTITION BY Cust_ID ORDER BY Order_Date), 
             Order_Date) AS MonthlyTimeGap
FROM dbo.e_commerce_data;

SELECT *
FROM MonthlyTimeGap;

--5. Categorise customers using average time gaps. Choose the most fitted labeling model for you.
--> For example: 
--> Labeled as churn if the customer hasn't made another purchase in the months since they made their first purchase.
--> Labeled as regular if the customer has made a purchase every month.

    WITH PurchaseTimeGaps AS (
        SELECT
            cust_id,
            order_date,
            LAG(order_date) OVER (PARTITION BY cust_id ORDER BY order_date) AS previous_order_date,
            DATEDIFF(MONTH, LAG(order_date) OVER (PARTITION BY cust_id ORDER BY order_date), order_date) AS time_gap_months
        FROM
            dbo.e_commerce_data
    ),

    AverageTimeGaps AS (
        SELECT
            cust_id,
            AVG(time_gap_months) AS avg_time_gap
        FROM
            PurchaseTimeGaps
        WHERE
            time_gap_months IS NOT NULL 
        GROUP BY
            cust_id
    ),

    CustomerLabels AS (
        SELECT
            cust_id,
            avg_time_gap,
            CASE
                WHEN avg_time_gap <= 1 THEN 'regular'         
                WHEN avg_time_gap > 6 THEN 'churn'           
                ELSE 'irregular'                              
            END AS customer_label
        FROM
            AverageTimeGaps
    )
    SELECT *
    FROM CustomerLabels;



--PART 3: Month-Wise Retention Rate
--Find month-by-month customer retention rate since the start of the business.
--There are many different variations in the calculation of Retention Rate. But we will try to calculate the month-wise retention rate in this project.
--So, we will be interested in how many of the customers in the previous month could be retained in the next month. 
--Proceed step by step by creating “views”. You can use the view you got at the end of the Customer Segmentation section as a source.
--1. Find the number of customers retained month-wise. (You can use time gaps)
--2. Calculate the month-wise retention rate. 
--Month-Wise Retention Rate = 1.0 * Number of Customers Retained in The Current Month / Total Number of Customers in the Previous Month


CREATE VIEW MonthlyCustomerPurchases AS
SELECT
    cust_id,
    YEAR(order_date) AS purchase_year,
    MONTH(order_date) AS purchase_month,
    COUNT(*) AS purchase_count
FROM
    dbo.e_commerce_data
GROUP BY
    cust_id,
    YEAR(order_date),
    MONTH(order_date);


CREATE VIEW MonthlyCustomerRetention AS
SELECT
    current_month.purchase_year,
    current_month.purchase_month,
    COUNT(DISTINCT current_month.cust_id) AS retained_customers,
    COUNT(DISTINCT previous_month.cust_id) AS total_customers_previous_month
FROM
    MonthlyCustomerPurchases AS current_month
LEFT JOIN
    MonthlyCustomerPurchases AS previous_month
ON
    current_month.cust_id = previous_month.cust_id
    AND current_month.purchase_year = previous_month.purchase_year
    AND current_month.purchase_month = previous_month.purchase_month + 1
GROUP BY
    current_month.purchase_year,
    current_month.purchase_month;
  

CREATE VIEW MonthlyRetentionRate AS
SELECT
    purchase_year,
    purchase_month,
    retained_customers,
    total_customers_previous_month,
    CASE 
        WHEN total_customers_previous_month > 0 THEN 
            1.0 * retained_customers / total_customers_previous_month
        ELSE 
            NULL 
    END AS retention_rate
FROM
    MonthlyCustomerRetention;


SELECT *
FROM MonthlyRetentionRate
ORDER BY purchase_year, purchase_month;