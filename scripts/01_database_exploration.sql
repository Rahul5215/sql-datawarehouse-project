/* =========================================================
   1. Explore Database Objects
========================================================= */
-- Explore all objects in the database
SELECT * FROM information_schema.tables;

-- Explore all columns in the database
SELECT * FROM information_schema.columns;

-- List only user-created tables
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
  AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;
