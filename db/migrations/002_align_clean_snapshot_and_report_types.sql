-- Align older Supabase deployments with v3 reporting schema.
-- Safe to run multiple times.

alter table public.clean_listings_snapshot
  add column if not exists property_category text,
  add column if not exists land_area numeric,
  add column if not exists land_area_unit text,
  add column if not exists building_area numeric,
  add column if not exists building_area_unit text;

create index if not exists idx_clean_snapshot_property_category
  on public.clean_listings_snapshot (property_category);

-- Optional backfill from payload JSON when upstream rows already contain category hints.
update public.clean_listings_snapshot
set property_category = lower(coalesce(payload->>'property_category', payload->>'property_type'))
where property_category is null
  and coalesce(payload->>'property_category', payload->>'property_type') is not null;
