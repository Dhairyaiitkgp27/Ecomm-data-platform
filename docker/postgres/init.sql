-- Runs once on first Postgres container start (mounted to docker-entrypoint-initdb.d).
-- Creates the schemas that Spark (staging) and dbt (marts/analytics/snapshots) use.
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS snapshots;
CREATE SCHEMA IF NOT EXISTS seeds;

-- The warehouse user (created by POSTGRES_USER env) owns everything it builds.
GRANT ALL ON SCHEMA staging, marts, analytics, snapshots, seeds TO CURRENT_USER;
