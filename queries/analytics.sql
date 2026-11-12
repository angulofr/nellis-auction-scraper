-- ==============================================================================
-- Nellis Auction Data Lake: Core Analytical Queries (DuckDB over Cloudflare R2)
-- Storage: s3://nellis-auction-lake/
-- Tables:
--   fact_auctions: Hive-partitioned daily lots (s3://nellis-auction-lake/fact_auctions/*/*/*.parquet)
--   dim_products: Deduplicated product catalog (s3://nellis-auction-lake/dim_products/products_catalog.parquet)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. Category & Brand Value Spreads
-- Rank categories and brands by lowest median discount_ratio (final_price / retail_price)
-- to identify segments with the greatest liquidation discount spreads.
-- ------------------------------------------------------------------------------
SELECT 
    COALESCE(NULLIF(p.category_primary, ''), 'Uncategorized') AS category,
    COALESCE(NULLIF(p.brand, ''), 'Generic') AS brand,
    COUNT(f.auction_id) AS total_lots_sold,
    ROUND(MEDIAN(f.discount_ratio), 4) AS median_discount_ratio,
    ROUND(AVG(f.retail_price), 2) AS avg_retail_price,
    ROUND(AVG(f.final_price), 2) AS avg_winning_bid,
    ROUND(AVG(f.bid_count), 1) AS avg_bids
FROM fact_auctions f
JOIN dim_products p ON f.product_id = p.product_id
WHERE f.discount_ratio IS NOT NULL 
  AND f.retail_price >= 20.0
GROUP BY category, brand
HAVING COUNT(f.auction_id) >= 5
ORDER BY median_discount_ratio ASC
LIMIT 30;


-- ------------------------------------------------------------------------------
-- 2. Condition Risk Arbitrage
-- Compare average and median closing prices of 'Brand New' vs. 'Open Box' vs.
-- 'Used' vs. 'Damaged' for identical product categories to calculate exact condition risk discounts.
-- ------------------------------------------------------------------------------
SELECT 
    f.condition,
    f.completeness,
    f.functional_status,
    COUNT(f.auction_id) AS lot_count,
    ROUND(AVG(f.retail_price), 2) AS avg_retail,
    ROUND(AVG(f.final_price), 2) AS avg_winning_bid,
    ROUND(AVG(f.discount_ratio), 4) AS avg_discount_ratio,
    ROUND(MEDIAN(f.discount_ratio), 4) AS median_discount_ratio,
    ROUND(AVG(f.bid_count), 1) AS avg_bids
FROM fact_auctions f
WHERE f.discount_ratio IS NOT NULL
GROUP BY f.condition, f.completeness, f.functional_status
ORDER BY lot_count DESC
LIMIT 25;


-- ------------------------------------------------------------------------------
-- 3. Temporal Sweet Spots
-- Aggregate discount_ratio and bid_count by close_day_of_week (0=Sun to 6=Sat)
-- and close_hour (0 to 23 Arizona MST) to find when bidder competition is lowest in Phoenix/Mesa.
-- ------------------------------------------------------------------------------
SELECT 
    f.close_day_of_week,
    CASE f.close_day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        ELSE 'Unknown'
    END AS day_name,
    f.close_hour,
    COUNT(f.auction_id) AS items_closing,
    ROUND(AVG(f.discount_ratio), 4) AS avg_discount_ratio,
    ROUND(MEDIAN(f.discount_ratio), 4) AS median_discount_ratio,
    ROUND(AVG(f.bid_count), 1) AS avg_bids_per_item
FROM fact_auctions f
WHERE f.close_day_of_week IS NOT NULL 
  AND f.close_hour IS NOT NULL
GROUP BY f.close_day_of_week, day_name, f.close_hour
ORDER BY f.close_day_of_week ASC, f.close_hour ASC;


-- ------------------------------------------------------------------------------
-- 4. Sleeper / Low-Bid High-Value Hunting
-- Filter for high-retail items (retail_price >= $150) that historically close
-- with low bid_count (<= 10 bids) and low discount_ratio (<= 0.25).
-- ------------------------------------------------------------------------------
SELECT 
    f.auction_id,
    p.title,
    f.location,
    f.condition,
    f.retail_price,
    f.final_price,
    ROUND(f.discount_ratio, 4) AS discount_ratio,
    f.bid_count,
    f.damage_notes,
    f.closed_at
FROM fact_auctions f
JOIN dim_products p ON f.product_id = p.product_id
WHERE f.retail_price >= 150.0
  AND f.bid_count <= 10
  AND f.discount_ratio <= 0.25
  AND f.condition IN ('Brand New', 'Like New', 'Open Box')
ORDER BY f.retail_price DESC
LIMIT 50;
