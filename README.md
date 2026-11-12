# Nellis Auction Data Lake & Analytics Pipeline

An automated, $0-cost serverless data lake and analytical query pipeline that scrapes completed and active auction lots from **Nellis Auction** (specifically targeting **Phoenix** and **Mesa, AZ** warehouses), stores them in a compressed columnar data lake on **Cloudflare R2**, and enables sub-second analytical SQL queries via **DuckDB**.

---

## 1. Architecture Overview

```text
                                  +-----------------------------+
                                  |   Nellis Auction API        |
                                  |   (Phoenix & Mesa, AZ)      |
                                  +--------------+--------------+
                                                 |
                                                 v
                                  +-----------------------------+
                                  |   GitHub Actions Runner     |
                                  |   (Scheduled 11:30 PM MST)  |
                                  +--------------+--------------+
                                                 |
                       +-------------------------+-------------------------+
                       |                                                   |
                       v                                                   v
        +-----------------------------+                     +-----------------------------+
        |   dim_products Transform    |                     |   fact_auctions Transform   |
        |   (SHA-256 Deduplication)   |                     |   (Pricing & Temporal MST)  |
        +--------------+--------------+                     +--------------+--------------+
                       |                                                   |
                       +-------------------------+-------------------------+
                                                 | (DuckDB / PyArrow ZSTD Parquet)
                                                 v
                                  +-----------------------------+
                                  |   Cloudflare R2 Bucket      |
                                  |   (s3://nellis-auction-lake)|
                                  +--------------+--------------+
                                                 |
                                                 | (DuckDB httpfs Range Requests)
                                                 v
                                  +-----------------------------+
                                  |   Analytical SQL Queries    |
                                  |   (Arbitrage, Sweet Spots)  |
                                  +-----------------------------+
```

---

## 2. Storage Layout (Hive-Partitioned Parquet)

Data is stored in **Cloudflare R2** under Hive-partitioned directories for high partition-pruning performance:

```text
s3://nellis-auction-lake/
├── dim_products/
│   └── products_catalog.parquet          # Deduplicated product catalog (upserted daily)
└── fact_auctions/
    └── year=2026/
        └── month=11/
            ├── auctions_2026-11-11.parquet
            ├── auctions_2026-11-12.parquet
            └── ...
```

---

## 3. Star Schema Design

### `dim_products` (Deduplicated Product Catalog)
Separates heavy text strings and descriptions from daily transaction facts to compress storage by ~60%:

| Column | Type | Description |
|---|---|---|
| `product_id` | `VARCHAR (PK)` | Deterministic SHA-256 hash of normalized title + retail price |
| `title` | `VARCHAR` | Full product name |
| `brand` | `VARCHAR` | Extracted / listed brand name |
| `category_primary` | `VARCHAR` | Primary category (`taxonomyLevel1`) |
| `category_secondary` | `VARCHAR` | Sub-category (`taxonomyLevel2`) |
| `base_retail_price` | `DECIMAL(10,2)`| Standard listed retail price |
| `description` | `TEXT` | Product descriptions & bullet features |
| `first_seen_date` | `DATE` | Date product was first scraped (`YYYY-MM-DD`) |

### `fact_auctions` (Daily Completed Lots)
Lean, numeric-heavy table accumulating ~30,000–35,000 rows/day:

| Column | Type | Description |
|---|---|---|
| `auction_id` | `BIGINT (PK)` | Unique Nellis lot ID |
| `product_id` | `VARCHAR (FK)` | Links to `dim_products.product_id` |
| `location` | `VARCHAR` | Warehouse location (`Phoenix`, `Mesa`) |
| `condition` | `VARCHAR` | Standardized condition (`Brand New`, `Used`, `Damaged`, `Untested`) |
| `completeness` | `VARCHAR` | `Complete`, `Missing Parts`, `Partial Set` |
| `functional_status`| `VARCHAR` | `Tested - Works`, `Untested`, `Not Functional` |
| `damage_notes` | `VARCHAR` | Raw inspector notes from Nellis team |
| `retail_price` | `DECIMAL(10,2)`| Retail price listed on this specific lot |
| `final_price` | `DECIMAL(10,2)`| Winning hammer price ($) |
| `discount_ratio` | `DECIMAL(5,4)` | Computed `final_price / NULLIF(retail_price, 0)` |
| `bid_count` | `INTEGER` | Total number of bids placed |
| `unique_bidders` | `INTEGER` | Estimated count of unique competing bidders |
| `closed_at` | `TIMESTAMP` | Exact closing timestamp |
| `close_day_of_week`| `TINYINT` | `0` (Sunday) through `6` (Saturday) |
| `close_hour` | `TINYINT` | `0` through `23` (Arizona MST hour) |
| `scraped_date` | `DATE` | Partition date (`YYYY-MM-DD`) |

---

## 4. Querying the Data Lake with DuckDB

DuckDB connects directly to Cloudflare R2 using the `httpfs` extension and executes sub-second queries over the Parquet files without downloading the entire dataset:

### Running Pre-Built Analytical Models
```powershell
# 1. Category Value Spreads (Categories & brands with lowest median discount ratio)
python -m src.query_lake --query category-spread

# 2. Condition Risk Arbitrage (Brand New vs. Used vs. Damaged pricing discount)
python -m src.query_lake --query condition-arbitrage

# 3. Temporal Sweet Spots (Day of week & hour competition heatmaps)
python -m src.query_lake --query temporal

# 4. Sleeper / Low-Bid Hunting (Retail > $150, Bids <= 10, Discount Ratio <= 0.25)
python -m src.query_lake --query sleepers
```

### Running Custom SQL Queries
```powershell
python -m src.query_lake --sql "SELECT p.title, f.final_price, f.retail_price, f.discount_ratio FROM fact_auctions f JOIN dim_products p ON f.product_id = p.product_id WHERE f.discount_ratio < 0.10 ORDER BY f.retail_price DESC LIMIT 10"
```

### Interactive SQL Shell
```powershell
python -m src.query_lake --interactive
```

---

## 5. Daily Ingestion Pipeline (GitHub Actions)

The workflow runs on GitHub Actions at `30 6 * * *` UTC (11:30 PM Arizona MST):
1. Executes concurrent requests to fetch all Mesa and Phoenix auction lots from the past 24 hours.
2. Normalizes records and updates `dim_products/products_catalog.parquet`.
3. Appends the day's batch to `fact_auctions/year=YYYY/month=MM/auctions_YYYY-MM-DD.parquet`.
4. Executes verification queries against the lake via DuckDB.
