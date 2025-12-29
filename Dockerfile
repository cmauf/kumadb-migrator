FROM mariadb:11

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip sqlite3 && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip3 install --no-cache-dir --break-system-packages mysql-connector-python

# Copy migration script
WORKDIR /app
COPY migrate.py /app/migrate.py
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Entry point starts MariaDB and then runs migration
ENTRYPOINT ["/app/entrypoint.sh"]
