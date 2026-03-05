# Supabase Schema and Migration Notes

Apply `db/migrations/001_supabase_market_tables.sql` in Supabase SQL editor or migration runner.

## Tables

1. `raw_listings`
   - Grain: one source listing payload per snapshot date.
   - Primary key: `(snapshot_date, source, listing_key)`.
   - Index: `(source, snapshot_date desc)`.

2. `clean_listings_snapshot`
   - Grain: normalized row per listing/day.
   - Primary key: `(snapshot_date, source, listing_key)`.
   - Indexes: `(source, snapshot_date desc)`, `(rent)`.

3. `daily_market_summary`
   - Grain: one metric row per source/day.
   - Primary key: `(snapshot_date, source, metric)`.
   - Index: `(source, snapshot_date desc)`.

## Idempotency

Loader upserts by:
- `snapshot_date,source,listing_key` for raw and clean listing tables.
- `snapshot_date,source,metric` for summary table.

This allows safe re-runs for the same date/source without duplicate rows.
