-- Re-apply SELECT grants after shadow-reset (role already exists).

GRANT CONNECT ON DATABASE shop TO pgflight_shadow;
GRANT USAGE ON SCHEMA public TO pgflight_shadow;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pgflight_shadow;