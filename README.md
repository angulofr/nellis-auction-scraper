# Nellis Auction Scraper & Finalizer (Phoenix & Mesa, AZ)

Automated Python scraper and GitHub Actions workflow to scrape, track, and analyze auction listings from [nellisauction.com](https://www.nellisauction.com) across **Phoenix** and **Mesa, AZ** warehouses into a local SQLite database.

## Architecture & Lifecycle

1. **Morning / Afternoon Catalog Scrape (10:00 AM MST)**:
   - Queries active listings across Phoenix & Mesa.
   - Captures item ID, title, description, condition, inspection notes, MSRP retail price, and scheduled closing time.
   - Inserts records into `auction_items` table with `is_closed = 0`.
2. **Night Finalization (~10:30 PM MST)**:
   - Queries all items in the database that reached or passed their closing time.
   - Fetches their direct endpoint to record the **official winning bid**, **final bid count**, and updates `is_closed = 1`, `market_status = 'sold'`.
   - Captures soft-close overtime extensions (`was_extended = 1`).

## Captured Attributes

| Attribute | Column | Description |
|---|---|---|
| **Closing Date** | `closing_date` | Formatted Arizona local time (`YYYY-MM-DD HH:MM:SS MST`) |
| **Title** | `title` | Listing title |
| **Description** | `description` | Formatted condition, functionality, and inspector notes |
| **Bids** | `bid_total` | Total bids placed |
| **Winning Bid** | `winning_bid_amount` | Current or winning bid amount ($) |
| **Retail Price** | `estimated_retail_price` | Suggested retail price ($) |
| **Discount %** | `discount_pct` | Calculated percentage discount vs retail |
| **Inventory #** | `inventory_number` | Unique Nellis inventory number |
| **Condition** | `condition_type` | `New`, `Used`, `Open Box` |
| **Star Rating** | `condition_rating` | Numerical condition rating (1 to 5) |
| **Functional?** | `is_functional` | `Yes`, `No`, `Untested` |
| **Damage** | `damage_type` | `None`, `Minor`, `Major` |
| **Missing Parts** | `missing_parts` | `No`, `Partial Set`, `Missing Parts` |
| **In Package** | `in_package` | `Yes`, `No` |
| **Assembly** | `assembly_required` | `Yes`, `No` |
| **Inspector Notes**| `inspector_notes` | Raw notes from Nellis inspection crew |
| **Status** | `market_status` | `open`, `sold`, `closed`, `relisted` |
| **Location** | `location_name` | `Phoenix` or `Mesa` |
| **Item URL** | `item_url` | Direct link to product listing |
| **Photos** | `photo_count`, `photo_url`, `all_photos_json` | Primary URL and JSON array of all inspection photos |

## Running Locally

```powershell
# Scrape all active items in Phoenix and Mesa
python nellis_scraper.py

# Finalize items whose close time has passed
python nellis_scraper.py --finalize

# Test run with limit
python nellis_scraper.py --limit 50

# Export to CSV
python nellis_scraper.py --export-csv nellis_auctions.csv
```

## GitHub Actions Automation

The included workflow runs automatically on GitHub:
- **10:00 AM MST (17:00 UTC)**: Daily Catalog Scrape
- **10:30 PM MST (05:30 UTC)**: Daily Finalization
- **Manual Trigger**: Can be run on-demand anytime from the **Actions** tab in GitHub!
