--Query 1: Calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
SELECT    FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) month
          ,SUM(totals.visits) visits
          ,SUM(totals.pageviews) pageviews
          ,SUM(totals.transactions) transactions
FROM      `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE     _table_suffix between '0101' and '0331'
GROUP BY   month
ORDER BY   1 ASC;

--Query 2: Bounce rate per traffic source in July 2017 
SELECT    trafficSource.source source
          ,SUM(totals.visits) total_visits
          ,SUM(totals.bounces) total_no_of_bounces
          ,ROUND(100.0*SUM(totals.bounces)/SUM(totals.visits),3) bounce_rate
FROM      `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE     _table_suffix between '0701' and '0731'
GROUP BY  source
ORDER BY  total_visits DESC;

--Query 3: Revenue by traffic source by week, by month in June 2017
WITH week AS (
    SELECT     'Week' time_type
              ,FORMAT_DATE('%Y%W',PARSE_DATE('%Y%m%d',date)) time
              ,trafficSource.source source
              ,ROUND(SUM(product.productRevenue)/1000000,4) revenue
    FROM      `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST    (hits) hits,
    UNNEST    (hits.product) product
    WHERE     _table_suffix between '0601' and '0630'
      AND     product.productRevenue IS NOT NULL
    GROUP BY  source, time
)
, month AS (
    SELECT     'Month' time_type
              ,FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) time
              ,trafficSource.source source
              ,ROUND(SUM(product.productRevenue)/1000000,4) revenue
    FROM      `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST    (hits) hits,
    UNNEST    (hits.product) product
    WHERE     _table_suffix between '0601' and '0630'
      AND     product.productRevenue IS NOT NULL
    GROUP BY  source, time
)
SELECT      time_type, time, source, revenue
FROM        week
UNION ALL
SELECT      time_type, time, source, revenue
FROM        month
ORDER BY    source ASC, time_type ASC, time ASC;

--Query 4: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.
WITH raw AS (
    SELECT     FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) month
              ,fullVisitorId user_id
              ,totals.pageviews pageviews
              ,totals.transactions transactions
              ,product.productRevenue revenue
    FROM      `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST    (hits) hits,
    UNNEST    (hits.product) product
    WHERE     _table_suffix between '0601' and '0731'
)
SELECT         month
            ,  SUM(CASE WHEN transactions >=1 AND revenue IS NOT NULL THEN pageviews END)/
                COUNT(DISTINCT CASE WHEN transactions >=1 AND revenue IS NOT NULL THEN user_id END) AS avg_pageviews_purchase
            ,  SUM(CASE WHEN transactions IS NULL AND revenue IS NULL THEN pageviews END)/
                COUNT(DISTINCT CASE WHEN transactions IS NULL AND revenue IS NULL THEN user_id END) AS avg_pageviews_non_purchase             
FROM           raw
GROUP BY       month
ORDER BY       month ASC;

--Query 5: Average number of transactions per user that made a purchase in July 2017
SELECT     FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) Month
          ,SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId) Avg_total_transactions_per_user
FROM      `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST    (hits) hits,
UNNEST    (hits.product) product
WHERE     _table_suffix between '0701' and '0731'
  AND     product.productRevenue IS NOT NULL
  AND     totals.transactions >= 1
GROUP BY  Month;

--Query 6: Average amount of money spent per session. Only include purchaser data in July 2017
SELECT     FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) Month
          ,ROUND(SUM(product.productRevenue)/1000000/SUM(totals.visits),2) avg_revenue_by_user_per_visit
FROM      `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST    (hits) hits,
UNNEST    (hits.product) product
WHERE     _table_suffix between '0701' and '0731'
  AND     product.productRevenue IS NOT NULL
  AND     totals.transactions IS NOT NULL
GROUP BY  Month;

--Query 7: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. 
WITH list_user AS (
    SELECT     fullVisitorId user_id
    FROM      `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST    (hits) hits,
    UNNEST    (hits.product) product
    WHERE     _table_suffix between '0701' and '0731'
      AND     product.productRevenue IS NOT NULL
      AND     product.v2ProductName LIKE 'YouTube Men_s Vintage Henley'
    GROUP BY  user_id
)

SELECT     product.v2ProductName other_purchased_products
          ,SUM(product.productQuantity) quantity
FROM       list_user AS l
INNER JOIN `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` AS a
ON         l.user_id = a.fullVisitorId,
UNNEST    (hits) hits,
UNNEST    (hits.product) product
WHERE     _table_suffix between '0701' and '0731'
  AND     product.productRevenue IS NOT NULL
  AND     product.v2ProductName NOT LIKE 'YouTube Men_s Vintage Henley'
GROUP BY  other_purchased_products
ORDER BY  quantity DESC;

--Query 8: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017.
WITH raw AS (
    SELECT     FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) month
              ,SUM(CASE WHEN hits.eCommerceAction.action_type = '2' THEN 1 ELSE 0 END) num_product_view
              ,SUM(CASE WHEN hits.eCommerceAction.action_type = '3' THEN 1 ELSE 0 END) num_addtocart
              ,SUM(CASE WHEN hits.eCommerceAction.action_type = '6' AND product.productRevenue IS NOT NULL THEN 1 ELSE 0 END) num_purchase
    FROM      `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST    (hits) hits,
    UNNEST    (hits.product) product
    WHERE     _table_suffix between '0101' and '0331'
    GROUP BY  month
    ORDER BY  month ASC
)
SELECT   month
        ,num_product_view
        ,num_addtocart
        ,num_purchase
        ,ROUND(100.0*num_addtocart/num_product_view,2) add_to_cart_rate
        ,ROUND(100.0*num_purchase/num_product_view,2) purchase_rate
FROM    raw;