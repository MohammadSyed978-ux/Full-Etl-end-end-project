## Mohammad Syed Full ETL end - end project analytical questions ##

/* ============================================================
   1) Find the top 10 revenue-generating products
   Goal:
   - Identify which products generate the highest total revenue
   - Revenue is calculated by summing sale_price per product
   ============================================================ */

SELECT
    product_id,
    ROUND(SUM(sale_price), 2) AS tot_rev
FROM orders
GROUP BY product_id
ORDER BY tot_rev DESC
LIMIT 10;



/* ============================================================
   2) Find which category is being sold in which region
      where total quantity sold is greater than 2000
   Goal:
   - Understand demand by region and category
   - Filter to only high-volume category-region combinations
   ============================================================ */

WITH filt AS (
    SELECT
        region,
        category,
        SUM(quantity) AS tot_quantity
    FROM etlproject.orders
    GROUP BY region, category
)
SELECT *
FROM filt
WHERE tot_quantity > 2000
ORDER BY tot_quantity DESC;



/* ============================================================
   3) Top 5 selling products in each region (by revenue)
   Goal:
   - Rank products within each region by total revenue
   - Return only the top 5 products per region
   ============================================================ */

WITH filt AS (
    SELECT
        product_id,
        region,
        ROUND(SUM(sale_price), 2) AS tot_rev
    FROM orders
    GROUP BY product_id, region
),
kilt AS (
    SELECT
        product_id,
        region,
        tot_rev,
        ROW_NUMBER() OVER (
            PARTITION BY region
            ORDER BY tot_rev DESC
        ) AS tot_rank
    FROM filt
)
SELECT *
FROM kilt
WHERE tot_rank <= 5
ORDER BY region, tot_rev DESC;



/* ============================================================
   4) For each category, find the month with the highest sales
   Goal:
   - Aggregate sales by category and year-month
   - Rank months within each category
   - Return the best-performing month per category
   ============================================================ */

WITH filt AS (
    SELECT
        category,
        DATE_FORMAT(order_date, '%Y%m') AS o_year_month,
        ROUND(SUM(sale_price), 2) AS tot_sales
    FROM etlproject.orders
    GROUP BY category, o_year_month
),
kilt AS (
    SELECT
        category,
        o_year_month,
        tot_sales,
        ROW_NUMBER() OVER (
            PARTITION BY category
            ORDER BY tot_sales DESC
        ) AS tot_rank
    FROM filt
)
SELECT *
FROM kilt
WHERE tot_rank = 1
ORDER BY tot_sales DESC;



/* ============================================================
   5) Month-over-month sales comparison for 2022 vs 2023
      (Only months with positive growth)
   Goal:
   - Compare monthly sales between two years
   - Pivot 2022 and 2023 sales side-by-side
   - Calculate variance and keep only growth months
   ============================================================ */

WITH filt AS (
    SELECT
        YEAR(order_date) AS o_year,
        MONTH(order_date) AS o_month,
        ROUND(SUM(sale_price), 0) AS tot_sales
    FROM orders
    GROUP BY o_year, o_month
),
kilt AS (
    SELECT
        o_month,
        SUM(CASE WHEN o_year = 2022 THEN tot_sales ELSE 0 END) AS sales_2022,
        SUM(CASE WHEN o_year = 2023 THEN tot_sales ELSE 0 END) AS sales_2023
    FROM filt
    GROUP BY o_month
),
jilt AS (
    SELECT
        o_month,
        sales_2022,
        sales_2023,
        sales_2023 - sales_2022 AS variance
    FROM kilt
)
SELECT *
FROM jilt
WHERE variance > 0
ORDER BY o_month;



/* ============================================================
   6) Which sub-category in each state generates the most profit
   Goal:
   - Aggregate profit by state and sub-category
   - Rank sub-categories within each state
   - Return the most profitable one per state
   ============================================================ */

WITH filt AS (
    SELECT
        state,
        sub_category,
        ROUND(SUM(profit), 2) AS profit
    FROM etlproject.orders
    GROUP BY state, sub_category
),
kilt AS (
    SELECT
        state,
        sub_category,
        profit,
        ROW_NUMBER() OVER (
            PARTITION BY state
            ORDER BY profit DESC
        ) AS rnked
    FROM filt
)
SELECT *
FROM kilt
WHERE rnked = 1
ORDER BY profit DESC;
