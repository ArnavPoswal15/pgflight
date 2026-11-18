-- SELECT-only shadow role for EXPLAIN/replay (ADR-003).
-- Mounted on postgres-shadow only, after seed.sql (02- prefix).

CREATE ROLE pgflight_shadow
  LOGIN
  PASSWORD 'pgflight_shadow'
  NOSUPERUSER
  NOCREATEDB
  NOCREATEROLE
  NOINHERIT;

ALTER ROLE pgflight_shadow SET default_transaction_read_only = on;
ALTER ROLE pgflight_shadow SET statement_timeout = '30s';

GRANT CONNECT ON DATABASE shop TO pgflight_shadow;
GRANT USAGE ON SCHEMA public TO pgflight_shadow;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pgflight_shadow;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
  GRANT SELECT ON TABLES TO pgflight_shadow;