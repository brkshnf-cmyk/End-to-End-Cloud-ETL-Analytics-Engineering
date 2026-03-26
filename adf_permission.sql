-- ============================================================
-- Script Title: Grant ADF Service Principal Access to SQL Database
--
-- Purpose:
--   This script provisions the Azure Data Factory (ADF) managed identity
--   as a SQL user and assigns it the necessary permissions to read from
--   and write to the database. This enables ADF pipelines to execute
--   queries, perform lookups, and load data during ETL operations.
--
-- Business Context:
--   ADF needs controlled access to the SQL database to support ingestion,
--   transformation, and loading processes within the Brazilian E‑Commerce
--   analytics pipeline. Granting least‑privilege roles ensures secure and
--   auditable data operations.
--
-- Technical Summary:
--   1. Creates a SQL user mapped to the ADF managed identity.
--   2. Adds the user to db_datareader for SELECT permissions.
--   3. Adds the user to db_datawriter for INSERT/UPDATE/DELETE permissions.
--
-- Notes:
--   - This script must be executed by a user with ALTER ANY USER permissions.
--   - The identity [adf-brazil] must already exist in Azure AD.
-- ============================================================

CREATE USER [adf-brazil] FROM EXTERNAL PROVIDER;

ALTER ROLE db_datareader ADD MEMBER [adf-brazil];
ALTER ROLE db_datawriter ADD MEMBER [adf-brazil];