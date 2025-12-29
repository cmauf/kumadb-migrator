#!/bin/bash
set -euo pipefail

# Required env vars
: "${MARIADB_USER:?missing}"
: "${MARIADB_PASSWORD:?missing}"

echo "==> Initializing MariaDB datadir if needed"
if [ ! -d /var/lib/mysql/mysql ]; then
    mariadb-install-db --user=mysql --datadir=/var/lib/mysql
fi

echo "==> Starting MariaDB"
/usr/bin/mariadbd-safe --datadir=/var/lib/mysql --socket=/run/mysqld/mysqld.sock &
MARIADB_PID=$!

# Wait for DB to be ready
until mariadb-admin ping --socket=/run/mysqld/mysqld.sock --silent; do
    sleep 1
done

echo "==> Creating database and user"
mariadb --socket=/run/mysqld/mysqld.sock <<EOF
CREATE DATABASE IF NOT EXISTS kumadb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS '${MARIADB_USER}'@'%' IDENTIFIED BY '${MARIADB_PASSWORD}';
GRANT ALL PRIVILEGES ON kumadb.* TO '${MARIADB_USER}'@'%';
FLUSH PRIVILEGES;
EOF

echo "==> Running migration script"
python3 /app/migrate.py

echo "==> Shutting down MariaDB"
mariadb-admin shutdown --socket=/run/mysqld/mysqld.sock
wait "$MARIADB_PID"
