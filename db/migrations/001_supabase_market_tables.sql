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
