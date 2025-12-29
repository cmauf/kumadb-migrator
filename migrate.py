"""
Vendored from
https://github.com/harshavmb/sqlite3tomysql/commit/2f5f1f28add5d86a03b0ca3db92b5aa66639065a
to use env variables. Also split up into atomic functions and removed MySQL clutter.
"""

import sqlite3
import re  # For regular expressions to parse types
from datetime import datetime
from os import getenv
import sys
from typing import Optional
import mysql.connector

MARIADB_USER = getenv("MARIADB_USER", "kuma")  ## database user
MARIADB_PASSWORD = getenv("MARIADB_PASSWORD", "secret")  ### password

DB: dict[str, Optional[object]] = {
    "sqlite_conn": None,
    "mysql_conn": None,
    "sqlite_cursor": None,
    "mysql_cursor": None
}

# Mapping rules for SQLite -> MySQL types
INTEGER_TYPES = {
    "TINYINT": "TINYINT",
    "SMALLINT": "SMALLINT",
    "MEDIUMINT": "MEDIUMINT",
    "BIGINT": "BIGINT UNSIGNED",
    "INT": "INT UNSIGNED",  # fallback
}

NUMERIC_TYPES = (("REAL", "FLOA", "DOUB"), "DOUBLE"), \
                (("NUM", "DEC"), "DECIMAL(10,2)"), \
                (("BOOL",), "TINYINT(1)")

TEXT_TYPES = ("CHAR", "CLOB", "TEXT")
BLOB_TYPES = ("BLOB",)

INDEXED_VARCHAR_MAX = 191
DEFAULT_VARCHAR_MAX = 255

def map_integer_type(sqlite_type_upper: str) -> str | None:
    """Map integer-like types."""
    for key, mysql_type in INTEGER_TYPES.items():
        if key in sqlite_type_upper:
            return mysql_type
    return None

def map_numeric_type(sqlite_type_upper: str) -> str | None:
    """Map REAL/NUMERIC/BOOLEAN types."""
    for keys, mysql_type in NUMERIC_TYPES:
        if any(k in sqlite_type_upper for k in keys):
            return mysql_type
    return None

def map_text_type(sqlite_type_raw: str, is_primary_key=False, is_unique=False) -> str | None:
    """Map CHAR/TEXT/CLOB types."""
    sqlite_type_upper = sqlite_type_raw.upper()
    if not any(k in sqlite_type_upper for k in TEXT_TYPES):
        return None

    if is_primary_key or is_unique:
        print(
            f"Warning: Indexed text column '{sqlite_type_raw}' mapped to "
            f"VARCHAR({INDEXED_VARCHAR_MAX}) for index compatibility.")
        return f"VARCHAR({INDEXED_VARCHAR_MAX})"

    match = re.search(r'\((\d+)\)', sqlite_type_raw)
    if match:
        length = int(match.group(1))
        return f"VARCHAR({min(length, DEFAULT_VARCHAR_MAX)})"

    return "LONGTEXT"

def map_blob_type(sqlite_type_raw: str, is_primary_key=False, is_unique=False) -> str | None:
    """Map BLOB types."""
    if "BLOB" not in sqlite_type_raw.upper():
        return None

    if is_primary_key or is_unique:
        print(
            f"Warning: Indexed BLOB column '{sqlite_type_raw}' mapped to "
            f"VARBINARY({INDEXED_VARCHAR_MAX}) for index compatibility.")
        return f"VARBINARY({INDEXED_VARCHAR_MAX})"

    return "BLOB"

def map_datetime_type(sqlite_type_upper: str) -> str | None:
    """Handle DATE, DATETIME, TIME types."""
    if sqlite_type_upper == "TIME":
        return "TIME"
    if "DATE" in sqlite_type_upper:
        return "DATETIME"
    return None

def map_sqlite_to_mysql_type(sqlite_type_raw, is_primary_key=False, is_unique=False):
    """
    Maps SQLite data types to MySQL types in a clean, maintainable way.
    """
    sqlite_type_upper = sqlite_type_raw.upper()

    # Try mapping in order of priority
    for mapper in (
        map_integer_type,
        lambda t: map_text_type(sqlite_type_raw, is_primary_key, is_unique),
        lambda t: map_blob_type(sqlite_type_raw, is_primary_key, is_unique),
        map_numeric_type,
        map_datetime_type
    ):
        mysql_type = mapper(sqlite_type_upper)
        if mysql_type:
            return mysql_type

    print(f"Warning: Unknown SQLite type '{sqlite_type_raw}'. Defaulting to VARCHAR(255).")
    return "VARCHAR(255)"


def establish_db_connections(sqlite_db_path, mysql_config):
    """ Establish DB connections, exit on error"""
    try:
        DB["sqlite_conn"] = sqlite3.connect(sqlite_db_path)
        DB["sqlite_cursor"] = DB["sqlite_conn"].cursor()
        print(f"Connected to SQLite database: {sqlite_db_path}")
    except sqlite3.Error as e:
        sys.exit(f"Error connecting to SQLite: {e}")

    try:
        DB["mysql_conn"] = mysql.connector.connect(**mysql_config)
        DB["mysql_cursor"] = DB["mysql_conn"].cursor()
        print(f"Connected to MySQL database: {mysql_config['database']}")
    except mysql.connector.Error as e:
        if DB["sqlite_cursor"]:
            DB["sqlite_cursor"].close()
        if DB["sqlite_conn"]:
            DB["sqlite_conn"].close()
        sys.exit(f"Error connecting to MySQL: {e}")

    try:
        DB["mysql_cursor"].execute("SET FOREIGN_KEY_CHECKS = 0;")
        DB["mysql_conn"].commit()
        print("Disabled MySQL foreign key checks.")
    except mysql.connector.Error as err:
        print(f"Error disabling foreign key checks: {err}")


def build_default_sql(default_value, mysql_type, table_name, col_name):
    """
    Build a MySQL/MariaDB DEFAULT clause from a SQLite default value.
    Returns (default_sql, possibly_modified_mysql_type)
    """

    # 1) No default at all
    if default_value is None:
        return "", mysql_type

    # Normalize string form once
    default_str_raw = str(default_value)
    default_str = default_str_raw.upper().replace('"', "'")

    # 2) CURRENT_TIMESTAMP / DATETIME('now') handling
    if (
        default_str == "CURRENT_TIMESTAMP"
        or default_str == "'CURRENT_TIMESTAMP'"
        or "DATETIME('NOW')" in default_str
    ):
        # MariaDB supports DEFAULT CURRENT_TIMESTAMP for DATETIME,
        # so we do NOT need to coerce to TIMESTAMP anymore.
        return " DEFAULT CURRENT_TIMESTAMP", mysql_type

    # 3) Explicit NULL defaults
    if default_str in ("NULL", "'NULL'"):
        return " DEFAULT NULL", mysql_type

    # 4) Numeric defaults (int / float / numeric strings)
    is_numeric = False
    try:
        numeric_value = float(default_value)
        is_numeric = True
    except (TypeError, ValueError):
        pass

    if is_numeric:
        # Handle TINYINT overflow
        if "TINYINT" in mysql_type:
            # Signed TINYINT range: -128..127
            if numeric_value < -128 or numeric_value > 127:
                print(
                    f"Warning: Default value {default_value} for TINYINT column "
                    f"'{table_name}.{col_name}' exceeds range. "
                    f"Promoting column to SMALLINT."
                )
                mysql_type = mysql_type.replace("TINYINT", "SMALLINT")

        return f" DEFAULT {default_value}", mysql_type

    # 5) String defaults
    # Strip surrounding quotes to avoid double-quoting
    cleaned = default_str_raw.strip("'\"")

    # Escape single quotes for SQL safety
    cleaned = cleaned.replace("'", "''")

    return f" DEFAULT '{cleaned}'", mysql_type


def set_ai_nns(pk, mysql_type, not_null, col_name, table_name):
    """
    Use MySQL type and not_null bool to infer auto_increment and not_null_sql.
    :param pk: primary key
    :param mysql_type: mysql type
    :param not_null: true/false (TINYINT)
    :param col_name:  column name
    :param table_name:  table name
    :return: auto_increment, not_null_sql
    """
    auto_increment = ""
    if pk == 1 and ("INT" in mysql_type or "BIGINT" in mysql_type):
        auto_increment = " AUTO_INCREMENT"
        if not_null == 0:
            print(
                f"Warning: Primary key '{col_name}' in table '{table_name}' is NULLABLE\
                 in SQLite. MySQL AUTO_INCREMENT implies NOT NULL.")
        not_null_sql = " NOT NULL"
    elif not_null == 1:
        not_null_sql = " NOT NULL"
    else:
        not_null_sql = ""
    return auto_increment, not_null_sql


def process_columns(col, table_name, col_defs, pk_col_names, primary_keys):
    """
    Process column definitions.
    :param col: The Column instance
    :param table_name: Name of the table
    :param col_defs: Column definitions list from context
    :param pk_col_names: Primary keys column names list from context
    :param primary_keys: Primary Keys list from context
    :return:
    """

    # Determine if column has a UNIQUE constraint.
    # This simple check is for single-column UNIQUE.
    # For complex migrations, parsing CREATE statement is better.
    is_unique_col = False
    # Handle reserved keywords in SQLite queries
    if table_name.lower() in {'group', 'order', 'key', 'index', 'table'}:
        DB["sqlite_cursor"].execute(f"PRAGMA index_list(`{table_name}`);")
    else:
        DB["sqlite_cursor"].execute(f"PRAGMA index_list('{table_name}');")
    indexes = DB["sqlite_cursor"].fetchall()
    for idx in indexes:
        idx_name = idx[1]
        is_unique_idx = idx[2]  # 1 for unique, 0 for not
        if is_unique_idx == 1:
            DB["sqlite_cursor"].execute(f"PRAGMA index_info('{idx_name}');")
            idx_cols = DB["sqlite_cursor"].fetchall()
            # Check if single column index and matches current column
            if len(idx_cols) == 1 and idx_cols[0][2] == col[1]:
                is_unique_col = True
                break  # Found a unique index for this column

    mysql_type = map_sqlite_to_mysql_type(
        col[2], # sqlite_type
        is_primary_key=(col[1] in pk_col_names),
        is_unique=is_unique_col  # Pass is_unique flag
    )

    auto_increment, not_null_sql = set_ai_nns(col[5], mysql_type, col[3], col[1], table_name)

    # Handle default values. Special case for created_at/updated_at to manage in app.
    default_sql, mysql_type = build_default_sql(col[4], mysql_type, table_name, col[1])

    col_defs.append(
        f"`{col[1]}` {mysql_type}{not_null_sql}{default_sql}"
        f"{auto_increment}".strip()
    )
    if col[5] == 1:  # primary key
        primary_keys.append(f"`{col[1]}`")

    # Add UNIQUE constraint if the column was found to be unique
    # Don't add UNIQUE if it's already PK (PK implies unique)
    if is_unique_col and col[1] not in pk_col_names:
        col_defs.append(f"UNIQUE (`{col[1]}`)")


def knex_timestamp_conversion(rows):
    """
    Handle timestamp conversion for knex_migrations table
    :param rows: list of tuples representing rows
    :return: same rows list but with converted timestamps
    """
    processed_rows = []
    for row_data in rows:
        new_row = list(row_data)  # Convert tuple to list for modification
        if len(new_row) >= 4:
            # Convert Unix timestamp to MySQL DATETIME format
            migration_time = new_row[3]  # migration_time column
            if migration_time and str(migration_time).isdigit():
                # Convert from milliseconds to seconds if needed
                timestamp_val = int(migration_time)
                if timestamp_val > 4000000000:  # If > year 2096, likely milliseconds
                    timestamp_val = timestamp_val // 1000

                try:
                    dt = datetime.fromtimestamp(timestamp_val)
                    new_row[3] = dt.strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, OSError) as e:
                    print(f"Warning: Could not convert timestamp {migration_time}: {e}")
                    new_row[3] = None

        # Assume original 'created_at' and 'updated_at' are the last two columns.
        # This is a simplification; a robust solution would use original_col_names.
        processed_rows.append(tuple(new_row))

    return processed_rows


def copy_rows(table_name, escaped_table_name):
    """
    Copy rows from table name to escaped table name.
    :param table_name: table name
    :param escaped_table_name: backticked table name
    """
    rows = DB["sqlite_cursor"].fetchall()
    if rows:
        # Get column names to construct a proper INSERT statement
        if table_name.lower() in {'group', 'order', 'key', 'index', 'table'}:
            DB["sqlite_cursor"].execute(f"PRAGMA table_info(`{table_name}`);")
        else:
            DB["sqlite_cursor"].execute(f"PRAGMA table_info({table_name});")
        original_col_names = [col[1] for col in DB["sqlite_cursor"].fetchall()]

        # Adjust original_col_names and data based on MySQL's schema changes
        # For api_key, if 'created_at' is managed by app, we need to pass a value.
        # If 'updated_at' is auto-updated ON UPDATE, we can omit it on INSERT,
        # but providing NOW() is also fine and explicit.
        if table_name == "knex_migrations":
            rows = knex_timestamp_conversion(rows)

        placeholders = ','.join(['%s'] * len(original_col_names))
        # Use INSERT IGNORE to skip duplicate key errors and continue processing
        insert_stmt = (f"INSERT IGNORE INTO {escaped_table_name} "
                       f"({','.join(f'`{col}`' for col in original_col_names)}) "
                       f"VALUES ({placeholders})")
        print(f"Copying {len(rows)} rows to `{table_name}` using: {insert_stmt}")

        batch_size = 1000
        successful_batches = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]

            try:
                DB["mysql_cursor"].executemany(insert_stmt, batch)
                DB["mysql_conn"].commit()
                successful_batches += 1
            except mysql.connector.Error as err:
                print(
                    f"Error inserting data into `{table_name}` "
                    f"(batch {i // batch_size}, starting row {i}): {err}")
                DB["mysql_conn"].rollback()
                # Continue with next batch instead of breaking
                continue

        print(
            f"Successfully processed {successful_batches} batches out of "
            f"{(len(rows) + batch_size - 1) // batch_size} for table "
            f"`{table_name}`")
        print(f"Data copied to `{table_name}`.")
    else:
        print(f"No data to copy for table `{table_name}`.")


def migrate_table(table_name):
    """ Run migration for table `table_name`. """
    if table_name == 'sqlite_sequence':
        print(f"Skipping internal SQLite table: {table_name}")
        return
    if table_name.startswith('sqlite_autoindex_'):
        print(f"Skipping internal SQLite autoindex table: {table_name}")
        return

    print(f"\nProcessing table: `{table_name}`")

    escaped_table_name = f"`{table_name}`"  # Escape table name for safety

    # Handle reserved keywords in SQLite queries too
    if table_name.lower() in {'group', 'order', 'key', 'index', 'table'}:
        DB["sqlite_cursor"].execute(f"PRAGMA table_info(`{table_name}`);")
    else:
        DB["sqlite_cursor"].execute(f"PRAGMA table_info({table_name});")
    columns = DB["sqlite_cursor"].fetchall()
    col_defs = []
    primary_keys = []
    # unique_constraints = {}  # Stores {col_name: unique_group_name} if composite unique

    # Determine primary keys and unique columns for accurate type mapping and constraint
    # generation
    pk_col_names = {col[1] for col in columns if col[5] == 1}  # col[5] is 'pk'

    # Additional logic: Check for UNIQUE constraints from sqlite_master
    # (if relevant for other tables)
    # For simplicity for 'api_key' example, we'll assume UNIQUE means single-column unique
    # index. A more advanced script would parse CREATE TABLE statements from sqlite_master
    # to find composite unique keys.

    # For this specific api_key case: client_name and key_hash are UNIQUE.
    # We'll treat columns with UNIQUE constraints as 'is_unique=True' for type mapping.
    # This is a simplification; a full parser would be needed for complex multi-column
    # unique keys.
    # For now, if a column is explicitly marked UNIQUE in the SQLite DDL, this will handle
    # it. The provided api_key DDL indicates client_name and key_hash are UNIQUE.

    for col in columns:
        process_columns(col, table_name, col_defs, pk_col_names, primary_keys)

    if primary_keys:
        col_defs.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

    # FIX for Python 3.11+ f-string backslash issue
    joined_col_defs = ',\n    '.join(col_defs)

    # For older MySQL versions, adding ROW_FORMAT=DYNAMIC if supported might solve index
    # length issues.
    # However, reducing VARCHAR length is the more reliable cross-version solution.
    # If your MySQL version supports it, you could add: ROW_FORMAT=DYNAMIC
    # create_stmt = f"CREATE TABLE IF NOT EXISTS `{table_name}`
    # (\n    {joined_col_defs}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    # COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC;"

    # The most compatible CREATE TABLE statement based on our troubleshooting
    create_stmt = (f"CREATE TABLE IF NOT EXISTS {escaped_table_name} (\n"
                   f"    {joined_col_defs}\n) "
                   f"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")

    print(f"Generated CREATE TABLE statement:\n{create_stmt}")

    try:
        DB["mysql_cursor"].execute(f"DROP TABLE IF EXISTS {escaped_table_name};")
        DB["mysql_cursor"].execute(create_stmt)
        print(f"Table `{table_name}` created in MySQL.")
    except mysql.connector.Error as err:
        print(f"Error creating table `{table_name}`: {err}")
        return

    # Handle reserved keywords in SQLite SELECT queries
    if table_name.lower() in {'group', 'order', 'key', 'index', 'table'}:
        DB["sqlite_cursor"].execute(f"SELECT * FROM `{table_name}`")
    else:
        DB["sqlite_cursor"].execute(f"SELECT * FROM {table_name}")
    copy_rows(table_name, escaped_table_name)


def migrate_sqlite_to_mysql(sqlite_db_path, mysql_config):
    """
    Migrates a SQLite database to MySQL, including table schemas and data.
    """
    establish_db_connections(sqlite_db_path,mysql_config)

    try:
        DB["sqlite_cursor"].execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = DB["sqlite_cursor"].fetchall()
        print(f"Found tables in SQLite: {[t[0] for t in tables]}")

        for table_name_tuple in tables:
            migrate_table(table_name_tuple[0])  # table_name_tuple[0] is the table name
    # pylint: disable=broad-exception-caught
    except Exception as e:
        print(f"An unexpected error occurred during migration: {e}")
        if DB["mysql_conn"]:
            DB["mysql_conn"].rollback()

    finally:
        if DB["mysql_cursor"] and DB["mysql_conn"]:
            try:
                DB["mysql_cursor"].execute("SET FOREIGN_KEY_CHECKS = 1;")
                DB["mysql_conn"].commit()
                print("\nForeign key checks re-enabled in MySQL.")
            except mysql.connector.Error as err:
                print(f"Error re-enabling foreign key checks: {err}")

        if DB["mysql_cursor"]:
            DB["mysql_cursor"].close()
        if DB["mysql_conn"]:
            DB["mysql_conn"].close()
        if DB["sqlite_cursor"]:
            DB["sqlite_cursor"].close()
        if DB["sqlite_conn"]:
            DB["sqlite_conn"].close()
        print("Database connections closed.")


# --- Configuration ---
SQLITE_DB = 'kuma.db'  ## database file of sqlite
mysql_connection_config = {
    'host': "127.0.0.1",  ## change to remote mysql host
    'user': MARIADB_USER,  ## database user
    'password': MARIADB_PASSWORD,  ### password
    'database': "kumadb"  ## database name
}

# --- Run the migration ---
if __name__ == "__main__":
    migrate_sqlite_to_mysql(SQLITE_DB, mysql_connection_config)
