# Uptimekuma Migration SQLite to MariaDB

This project is aimed to provide a seamless migration of an Uptime Kuma database from SQLite to MariaDB.

## Preface

With V2, Uptime Kuma introduced support for MariaDB. It can run within the Docker Container, but also external
this project aims to provide a one-shot migration of an existing Uptime Kuma DB from SQLite to MariaDB. The resulting
MariaDB Database can then be used from within Uptime Kuma or a dedicated container.

## Prerequisites

- have Docker installed
- have an Uptime Kuma instance
- have the DB already upgraded to V2

## Use

- build the Docker Image: `docker build -t uptimekuma-db-migrator .`
- run the migration container: 
``` bash
docker run \
-e MARIADB_USER=$USER -e MARIADB_PASSWORD=$PASSWORD -v PATH/TO/kuma.db:/app/kuma.db \
-v PATH/TO/MARIADB/FILE/MOUNT:/var/lib/mysql uptimekuma-db-migrator
```

If run successfully, the container leaves a directory at `PATH/TO/MARIADB/FILE/MOUNT` that you can you to mount into a
MariaDB Container serving as your Uptime Kuma database.


## Acknowledgements

The central `migrate.py` is taken from [harshavmb's Project](https://github.com/harshavmb/sqlite3tomysql/). There were
modifications made by splitting the script in functions of smaller scope and removing fallbacks for old MySQL versions.


## Notes
- Stop your Kuma Instance before making a copy of `kuma.db`
- The resulting database is called `kumadb`
