SELECT format('CREATE DATABASE %I', 'superset')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'superset')
\gexec
