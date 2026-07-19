#!/bin/bash
set -e

echo "Initializing SHIFT database..."

# Wait for PostgreSQL to be ready
until PGPASSWORD=shift psql -h postgres -U shift -d postgres -c '\q' 2>/dev/null; do
  echo "Waiting for PostgreSQL..."
  sleep 1
done

echo "PostgreSQL is ready. Running migrations..."

# Run migrations
PGPASSWORD=shift psql -h postgres -U shift -d shift -f /migrations/schema.sql

echo "Database initialized successfully!"


