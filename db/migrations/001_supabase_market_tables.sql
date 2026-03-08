-- Supabase persistence tables for SouthportMarketAutomation

create table if not exists public.raw_listings (
  snapshot_date date not null,
  source text not null,
  listing_key text not null,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  primary key (snapshot_date, source, listing_key)
);

create index if not exists idx_raw_listings_source_date
  on public.raw_listings (source, snapshot_date desc);

create table if not exists public.clean_listings_snapshot (
  snapshot_date date not null,
  source text not null,
  listing_key text not null,
  rent numeric,
  bedrooms integer,
  size_sqft numeric,
  property_category text,
  land_area numeric,
  land_area_unit text,
  building_area numeric,
  building_area_unit text,
  payload jsonb not null,
  updated_at timestamptz not null default now(),
  primary key (snapshot_date, source, listing_key)
);

create index if not exists idx_clean_snapshot_source_date
  on public.clean_listings_snapshot (source, snapshot_date desc);

create index if not exists idx_clean_snapshot_rent
  on public.clean_listings_snapshot (rent);

create table if not exists public.daily_market_summary (
  snapshot_date date not null,
  source text not null,
  metric text not null,
  value text not null,
  created_at timestamptz not null default now(),
  primary key (snapshot_date, source, metric)
);

create index if not exists idx_market_summary_source_date
  on public.daily_market_summary (source, snapshot_date desc);

create table if not exists public.market_reports (
  snapshot_date date not null,
  source text not null,
  report_type text not null,
  report_version text not null,
  record_count integer not null default 0,
  report_markdown text not null,
  report_json jsonb not null,
  created_at timestamptz not null default now(),
  primary key (snapshot_date, source, report_type, report_version)
);

create index if not exists idx_market_reports_source_date
  on public.market_reports (source, snapshot_date desc);

create index if not exists idx_market_reports_type_version
  on public.market_reports (report_type, report_version, snapshot_date desc);
