-- Role Definitions & Least-Privilege Grants (Gate D2)
-- Database: network_monitoring

-- 1. Create Roles
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'raw_writer') THEN
        CREATE ROLE raw_writer NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'processed_writer') THEN
        CREATE ROLE processed_writer NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_user') THEN
        CREATE ROLE app_user WITH LOGIN PASSWORD 'app_secure_pass' NOSUPERUSER NOCREATEDB NOCREATEROLE;
    END IF;
END $$;

-- 2. Grant role memberships to the application login (non-superuser)
GRANT raw_writer TO app_user;
GRANT processed_writer TO app_user;

-- 3. Schema & Connection privileges
GRANT CONNECT ON DATABASE network_monitoring TO app_user;
GRANT USAGE ON SCHEMA public TO app_user;
GRANT USAGE ON SCHEMA public TO raw_writer;
GRANT USAGE ON SCHEMA public TO processed_writer;

-- 4. Grants for raw_writer (Gate D2: INSERT & SELECT for ON CONFLICT target, strictly NO UPDATE, NO DELETE)
REVOKE ALL ON TABLE raw_flows FROM raw_writer;
GRANT INSERT, SELECT ON TABLE raw_flows TO raw_writer;

-- 5. Grants for processed_writer
-- Needs SELECT and REFERENCES on raw_flows for foreign keys and window aggregation joins
GRANT SELECT, REFERENCES ON TABLE raw_flows TO processed_writer;
GRANT INSERT, UPDATE, SELECT ON TABLE flow_scores TO processed_writer;
GRANT INSERT, UPDATE, SELECT ON TABLE window_metrics TO processed_writer;
GRANT INSERT, UPDATE, SELECT ON TABLE model_runs TO processed_writer;
