CREATE TABLE raw.raw_customers (id SERIAL PRIMARY KEY, data JSONB, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE raw.raw_accounts  (id SERIAL PRIMARY KEY, data JSONB, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE raw.raw_transactions (id SERIAL PRIMARY KEY, data JSONB, created_at TIMESTAMPTZ DEFAULT now());
