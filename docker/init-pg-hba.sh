#!/bin/bash
# Initialize pg_hba.conf for external connections
set -e

# Wait for PostgreSQL to be ready
until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do
  sleep 1
done

# Backup original pg_hba.conf
cp /var/lib/postgresql/data/pg_hba.conf /var/lib/postgresql/data/pg_hba.conf.backup

# Create new pg_hba.conf with proper external access
cat > /var/lib/postgresql/data/pg_hba.conf <<EOF
# PostgreSQL Client Authentication Configuration File
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
local   replication     all                                     trust
host    replication     all             127.0.0.1/32            trust
host    replication     all             ::1/128                 trust
# Allow external connections (for Docker port forwarding)
host    all             all             0.0.0.0/0               md5
host    all             all             ::/0                    md5
EOF

# Reload configuration
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT pg_reload_conf();"

echo "pg_hba.conf configured for external access"
