# rockdbutil

Fast, reliable MariaDB/MySQL import/export tool with parallel processing, large-table chunked imports, selective restore via binlog, and automatic error recovery.

## Features

- Parallel processing for multiple tables
- Chunked parallel import for large tables (configurable threshold, streams, concurrency)
- Monolithic `.sql.gz` dump support via `sqlsplit.sh` - no manual pre-processing needed
- **Selective restore** - restore only tables that changed since last export, driven by binlog scanning
- **Binlog suppression during import** - prevents spurious binlog/slow-log growth on the server
- Automatic retry logic with deadlock/lock-timeout handling
- Cross-platform support (Arch, Manjaro, Ubuntu, Debian)
- Temporary InnoDB optimizations for faster imports (buffer pool, flush log, doublewrite, I/O capacity)
- Storage-type detection (NVMe / SATA SSD / HDD) for automatic I/O tuning
- Comprehensive error logging and per-table recovery
- Multiple named database profiles in a single config file

## Quick Start

Everything you need to go from zero to a working import in one pass. Do these steps in order.

---

### Step 1 - Configure MariaDB (`my.cnf`)

rockdbutil needs binlog enabled in ROW format to support selective restore. 
Add the following to your MariaDB config under `[mysqld]` if it isn't already there:

```ini
[mysqld]
log_bin = /var/log/mysql/mariadb-bin
binlog_format = ROW
expire_logs_days = 7
max_binlog_size = 100M
```

Restart MariaDB after editing:
```bash
# Arch / Manjaro
sudo systemctl restart mariadb

# Ubuntu / Debian
sudo systemctl restart mysql
```

> If you only need basic import/export and don't need selective restore, you can skip this step. All other features work without binlog.

---

### Step 2 - Grant database privileges

Connect to MariaDB as root and run all of the following for your import user:

```sql
-- Standard import/export operations
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER,
      LOCK TABLES, SHOW VIEW, EVENT, TRIGGER ON *.* TO 'your_user'@'localhost';

-- InnoDB optimizations during import (buffer pool, flush log, doublewrite, I/O capacity)
GRANT SUPER ON *.* TO 'your_user'@'localhost';

-- Binlog reading and suppression (required for selective restore)
GRANT REPLICATION CLIENT, REPLICATION SLAVE, BINLOG ADMIN ON *.* TO 'your_user'@'localhost';

FLUSH PRIVILEGES;
```

Replace `your_user` with your actual MariaDB username. 
If you already have `ALL PRIVILEGES` on the target database, you still need the `*.*` grants above - `SUPER` and `BINLOG ADMIN` cannot be granted per-database.

---

### Step 3 - Download the scripts

```bash
wget https://raw.githubusercontent.com/M3l-chior/rockdbutil/main/rockdbutil.sh
wget https://raw.githubusercontent.com/M3l-chior/rockdbutil/main/sqlsplit.sh
wget https://raw.githubusercontent.com/M3l-chior/rockdbutil/main/binlogparser.sh
chmod +x rockdbutil.sh sqlsplit.sh binlogparser.sh
```

Keep all three scripts in the same directory - they call each other by relative path.

---

### Step 4 - Run setup

```bash
./rockdbutil.sh --setup
```

This creates `~/.config/rockdbutil.conf` and the working directories under `~/database_operations/`.

---

### Step 5 - Edit the config

```bash
vim ~/.config/rockdbutil.conf
```

At minimum, set your default database credentials:

```bash
default_db_name=your_database
default_db_user=your_user
default_db_pass=your_password
default_db_host=localhost
default_db_port=3306
```

For multiple databases, add named profiles - see [Configuration](#configuration) for the full reference.

**Recommended tuning for NVMe / 16-core systems:**
```bash
large_table_threshold_mb=200
large_table_chunks=4
max_concurrent_large_tables=6
thread_mode=max
```

---

### Step 6 - Test the connection

```bash
./rockdbutil.sh --test-connection
```

---

### Step 7 - Import or export

**Import a dump:**
```bash
# From a previous rockdbutil export
./rockdbutil.sh -i db_dump_mydb_20260330.tar.gz -db your_profile

# From a raw mysqldump / mariadb-dump
./rockdbutil.sh -i db_backup.sql.gz -db your_profile
```

**Export your database:**
```bash
./rockdbutil.sh -e -db your_profile
```

The export records the current binlog position - this is the baseline for selective restore.

---

### Step 8 - Selective restore (after making changes)

Once you've exported a baseline, made changes to the database via your app, and want to revert only those changes:

```bash
./rockdbutil.sh --selective-restore -i db_dump_mydb_20260330.tar.gz -db your_profile
```

rockdbutil scans the binlog from the export position, finds only the tables that changed, and restores just those. See [Selective Restore](#selective-restore) for the full workflow.

---

## Prerequisites

Required (install manually):
- MariaDB or MySQL client tools (`mariadb` / `mysql`, `mariadb-dump` / `mysqldump`, `mariadb-binlog` / `mysqlbinlog`)
- Basic utilities: `tar`, `gzip`, `bc`, `awk`

Auto-installed by the script if missing:
- GNU Parallel

## Usage

```bash
./rockdbutil.sh --setup                                              # Initial setup
./rockdbutil.sh -e                                                   # Export default database
./rockdbutil.sh -e -db production                                    # Export using named profile
./rockdbutil.sh -e -d                                                # Export with auto-cleanup
./rockdbutil.sh -i backup.tar.gz                                     # Import rockdbutil export (default profile)
./rockdbutil.sh -i db_backup.sql.gz                                  # Import monolithic dump (default profile)
./rockdbutil.sh -i backup.tar.gz -db staging                         # Import to named profile
./rockdbutil.sh -i db_backup.sql.gz -db staging                      # Import monolithic dump to named profile
./rockdbutil.sh -d -i backup.tar.gz                                  # Import with auto-cleanup and optimization
./rockdbutil.sh -d -i backup.tar.gz -db production                   # Auto-optimized import to named profile
./rockdbutil.sh --selective-restore -i backup.tar.gz -db staging     # Selective restore (auto mode)
./rockdbutil.sh --selective-restore -i backup.tar.gz -db staging \
  --tables "tbl_policy,tbl_policy_status"                            # Selective restore (manual mode)
./rockdbutil.sh --list-profiles                                      # List all configured database profiles
./rockdbutil.sh --test-connection                                     # Test default profile connection
./rockdbutil.sh --test-connection -db production                     # Test named profile connection
```

### Flags

| Flag | Long form | Description |
|---|---|---|
| `-e` | `--export` | Export database to a compressed `.tar.gz` archive |
| `-i FILE` | `--import FILE` | Import from a `.tar.gz` (rockdbutil export) or `.sql.gz` (monolithic dump) |
| `-db PROFILE` | `--database PROFILE` | Use a named database profile (default: `default`) |
| `-d` | `--auto-cleanup` | Remove temporary files after completion; also enables buffer pool optimization non-interactively |
| `--selective-restore` | | Restore only tables changed since last export (requires `-i`) |
| `--tables TABLE,...` | | Comma-separated table list for manual selective restore (skips binlog scanning) |
| `--setup` | | Create the config file and working directories |
| `--list-profiles` | | List all profiles defined in the config file |
| `--test-connection` | | Test the database connection for the selected profile |
| `-h` | `--help` | Show help |

## Import File Types

rockdbutil accepts two input formats for `-i`:

| Format | Source | Behaviour |
|---|---|---|
| `.tar.gz` | A previous rockdbutil export | Extracted directly into the restore directory and imported in parallel |
| `.sql.gz` | A monolithic `mysqldump` / `mariadb-dump` | Passed to `sqlsplit.sh` which streams and splits it into per-table files, then imports in parallel - no intermediate full-size SQL file written to disk |

`sqlsplit.sh` must be present in the same directory as `rockdbutil.sh` for `.sql.gz` imports.

## Selective Restore

Selective restore uses the MariaDB binlog to identify exactly which tables changed since the last export, and restores only those - leaving untouched tables alone. This is significantly faster than a full reimport when only a small portion of the database has changed.

### How it works

Every export records the binlog position at the time of the dump into a `__export_meta.txt` file inside the archive. On selective restore, `binlogparser.sh` scans the binlog from that position forward to find changed tables. FK dependencies are resolved automatically - if a restored table has a foreign key pointing to a parent table, the parent is added to the restore set even if it wasn't in the changed list.

After each selective restore the binlog baseline is advanced to the current position, so subsequent restores only see changes made after the previous restore completed.

### Everyday workflow

```bash
# 1. Import fresh prod data (once)
./rockdbutil.sh -i prod_dump.sql.gz -db staging

# 2. Export to record the binlog baseline
./rockdbutil.sh -e -db staging

# 3. Start your app, make changes, stop the app

# 4. Selective restore - only changed tables are reimported
./rockdbutil.sh --selective-restore -i db_dump_staging_TIMESTAMP.tar.gz -db staging

# 5. Repeat from step 3 for the next dev session
```

### Modes

**Auto mode** - scans binlog automatically using the recorded export position:
```bash
./rockdbutil.sh --selective-restore -i backup.tar.gz -db staging
```

**Manual mode** - supply the table list directly, skips binlog scanning entirely:
```bash
./rockdbutil.sh --selective-restore -i backup.tar.gz -db staging --tables "tbl_policy,tbl_policy_status"
```

### Required privileges for selective restore

In addition to standard import privileges, the user needs:
```sql
GRANT REPLICATION CLIENT, REPLICATION SLAVE, BINLOG ADMIN ON *.* TO 'your_user'@'localhost';
FLUSH PRIVILEGES;
```

`BINLOG ADMIN` is required to suppress binlog writes during import (prevents the restore itself from being recorded as new binlog events). Without it, selective restore still works but will generate binlog entries for the reimported data.

## Binlog Suppression During Import

Full imports and selective restores temporarily disable binlog and slow query log writes for the duration of the import session. This prevents several GB of redundant binlog from being generated for data that already exists in the dump.

On a 6.5GB database, this alone reduced import time from 15 minutes to 8 minutes - the NVMe was doing double the write work when binlog was active.

Suppression is applied at the session level - only the import session is affected, other connections are unaffected. Both settings are restored to their original values when the import completes or if it fails.

## Performance

**Tested Performance Comparison:**

**6.5GB Database - 891MB Compressed (727 Tables):**
| Method | Time |
|---|---|
| Raw `mariadb < file.sql` | ~19 min |
| rockdbutil (binlog active) | 15 min |
| rockdbutil (binlog suppressed) | **8m 26s** |
| Selective restore (5.4GB changed) | **7m 11s** |

**Test System Specs:**
- **CPU:** 16 cores (16 parallel threads)
- **RAM:** 15GB total
- **Storage:** NVMe SSD (Micron 2210, 512GB)
- **OS:** Arch Linux
- **Buffer Pool:** Auto-optimized from 1GB -> 9GB during import

**Speed improvements from:**
- **Parallel table processing** - auto-detected thread count
- **Chunked parallel import** - multiple concurrent INSERT streams per large table
- **Temporary InnoDB optimizations** - buffer pool, flush log, doublewrite, I/O capacity
- **Binlog suppression** - eliminates redundant write amplification during import
- **Storage-type detection** - NVMe/SSD I/O capacity tuned automatically
- **Intelligent retry logic** - lock timeouts and deadlocks retried; chunk contention falls back to sequential automatically
- **Streaming SQL split** - `.sql.gz` dumps are never fully decompressed to disk

**Expected Performance on Different Hardware:**
- **NVMe SSD + 8+ cores:** Similar or better results
- **SATA SSD + 4–8 cores:** 30–40% improvement expected
- **Traditional HDD:** 20–30% improvement (limited by disk I/O; I/O capacity tuning skipped)
- **Low RAM systems:** Buffer pool scaling still provides benefit

*Results vary based on database structure, table count, hardware specs, and system load.*
*The script automatically adapts buffer pool size and thread count to your system.*

## Configuration

The configuration file is located at `~/.config/rockdbutil.conf`. Create it with `./rockdbutil.sh --setup`.

### Full Configuration Reference

```bash
# -------------------------------------------------------
# Database profiles
# Format: <profilename>_db_name, _db_user, _db_pass, _db_host, _db_port
# The profile name is the prefix before _db_name.
# Use -db <profilename> to select a profile at runtime.
# -------------------------------------------------------

# Default profile (used when no -db flag is supplied)
default_db_name=your_database
default_db_user=your_user
default_db_pass=your_password
default_db_host=localhost
default_db_port=3306

# Example: production profile
# production_db_name=prod_database
# production_db_user=prod_user
# production_db_pass=prod_password
# production_db_host=prod.company.com
# production_db_port=3306

# Example: staging profile
# staging_db_name=staging_db
# staging_db_user=staging_user
# staging_db_pass=staging_password
# staging_db_host=staging.company.com
# staging_db_port=3306

# -------------------------------------------------------
# Thread settings
# -------------------------------------------------------

# threads_override: set to a positive integer to fix the thread count.
# Set to 0 to let rockdbutil decide automatically.
threads_override=0

# thread_mode: controls how many threads are used when threads_override=0.
#   conservative - total CPU cores minus 2 (leaves headroom for the OS)
#   max          - all available CPU cores
thread_mode=max

# -------------------------------------------------------
# General settings
# -------------------------------------------------------

# auto_cleanup: if true, temporary files are always removed after import/export.
# If false (default), pass -d at runtime to clean up, or files are kept.
auto_cleanup=false

# base_directory: root for all working files (dumps, restore, logs).
base_directory=$HOME/database_operations

# -------------------------------------------------------
# Large table chunked import
# Tables whose .sql file exceeds large_table_threshold_mb are split into
# parallel INSERT streams instead of being imported as a single serial file.
# -------------------------------------------------------

# Threshold in MB above which a table is treated as "large"
large_table_threshold_mb=200

# Number of parallel INSERT streams per large table
large_table_chunks=4

# Number of large tables imported concurrently.
# Total concurrent streams = max_concurrent_large_tables × large_table_chunks
# e.g. 6 × 4 = 24 concurrent streams
max_concurrent_large_tables=6

# -------------------------------------------------------
# InnoDB import optimizations
# These are applied temporarily at the start of an import and restored
# to their original values when the import finishes (or if it fails).
# All optimizations require the database user to have SUPER privilege.
# -------------------------------------------------------

# innodb_flush_log_at_trx_commit=2 - flushes the redo log to the OS cache
# instead of to disk on every commit. Significant throughput gain during
# controlled imports. Safe to re-run from the dump if the import is interrupted.
# Set to false if crash safety during import is required.
innodb_flush_log_opt=true

# innodb_doublewrite=0 - disables the doublewrite buffer during import,
# halving InnoDB write amplification. Safe for controlled imports since
# you can re-run from the dump if interrupted.
# Set to false if crash safety during import is required.
innodb_doublewrite_opt=true

# innodb_io_capacity / innodb_io_capacity_max - tuned automatically based
# on detected storage type:
#   NVMe:    2000 / 8000
#   SSD:     1000 / 4000
#   HDD:     left at server default (no change applied)
# Set to false to disable and leave server defaults in place.
innodb_io_capacity_opt=true
```

### Profile Naming

The prefix before `_db_name` is the profile name used with the `-db` flag:

| Config key | Profile name |
|---|---|
| `default_db_name=...` | `default` (used when `-db` is omitted) |
| `production_db_name=...` | `production` |
| `staging_db_name=...` | `staging` |
| `client_a_db_name=...` | `client_a` |

## Multi-Database Usage

```bash
# List all configured profiles
./rockdbutil.sh --list-profiles

# Test connections
./rockdbutil.sh --test-connection -db production
./rockdbutil.sh --test-connection -db staging

# Export from a specific profile
./rockdbutil.sh -e -db production

# Import rockdbutil export to a specific profile
./rockdbutil.sh -i backup.tar.gz -db staging

# Import a monolithic prod dump to staging
./rockdbutil.sh -i prod_backup.sql.gz -db staging

# Import with auto-cleanup and buffer optimization
./rockdbutil.sh -d -i backup.tar.gz -db dev
```

## sqlsplit.sh - Standalone Usage

`sqlsplit.sh` is called automatically by rockdbutil when the import source is a `.sql.gz` file. It can also be used standalone to pre-split a dump into a portable `.tar.gz`:

```bash
# Split and package into a tar.gz in the current directory
./sqlsplit.sh db_backup.sql.gz

# Specify a custom output directory for the tar.gz
./sqlsplit.sh db_backup.sql.gz --output ~/splits/

# Override the database name (useful when the dump header lacks a USE statement)
./sqlsplit.sh db_backup.sql.gz --db-name my_database

# Remove the temporary working files after completion
./sqlsplit.sh db_backup.sql.gz --cleanup
```

The resulting `.tar.gz` can be imported with `./rockdbutil.sh -i` like any native export.

## binlogparser.sh - Standalone Usage

`binlogparser.sh` is called automatically by rockdbutil during selective restore. It can also be used standalone to inspect which tables changed in a given time window or since a specific binlog position:

```bash
# Show tables changed since a datetime
./binlogparser.sh --since '2026-03-30 08:00:00' --db-name mydb -db myprofile

# Show tables changed since last export (reads position from last_export_meta.txt)
./binlogparser.sh --from-export ~/database_operations/last_export_meta.txt --db-name mydb

# Start from a specific binlog file and position
./binlogparser.sh --from-pos mariadb-bin.000312:22642 --db-name mydb -db myprofile

# Write the table list to a file
./binlogparser.sh --since '2026-03-30 08:00:00' --db-name mydb --output changed_tables.txt
```

## Working Directories

All working files are written under `base_directory` (default: `~/database_operations/`):

| Path | Purpose |
|---|---|
| `~/database_operations/dumps/` | Per-table `.sql` files during export |
| `~/database_operations/restore/` | Per-table `.sql` files during import |
| `~/database_operations/logs/` | Per-table error logs |
| `~/database_operations/error_report.txt` | Summary of any failed imports |
| `~/database_operations/success_log.txt` | List of successfully imported tables |
| `~/database_operations/last_export_meta.txt` | Binlog position from last export - used by selective restore |

## Required Database Privileges

### Standard import/export
```sql
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER,
      LOCK TABLES, SHOW VIEW, EVENT, TRIGGER ON *.* TO 'your_user'@'localhost';
```

### InnoDB optimizations (recommended)
Allows the buffer pool, flush log, doublewrite, and I/O capacity to be tuned during import:
```sql
GRANT SUPER ON *.* TO 'your_user'@'localhost';
```

### Selective restore + binlog suppression (required for binlog features)
```sql
GRANT REPLICATION CLIENT, REPLICATION SLAVE, BINLOG ADMIN ON *.* TO 'your_user'@'localhost';
FLUSH PRIVILEGES;
```

`REPLICATION CLIENT` and `REPLICATION SLAVE` are needed to read the binlog remotely.
`BINLOG ADMIN` is needed to suppress binlog writes during import sessions - without it imports
still work but will generate several GB of redundant binlog entries for the reimported data.

## Troubleshooting

**Cannot connect to database**
Check your credentials in `~/.config/rockdbutil.conf` and confirm the MariaDB/MySQL service is running.

**`sqlsplit.sh` not found**
Both `rockdbutil.sh` and `sqlsplit.sh` must be in the same directory. The script expects `sqlsplit.sh` at the same path as itself.

**`sqlsplit.sh` not executable**
```bash
chmod +x sqlsplit.sh binlogparser.sh
```

**Missing `parallel` command**
```bash
# Arch / Manjaro
sudo pacman -S parallel

# Ubuntu / Debian
sudo apt install parallel
```

**Disk fills up during import**
The import generates binlog entries if `BINLOG ADMIN` is not granted. On large databases this
can be several GB. Grant `BINLOG ADMIN` (see above) and purge existing binlogs before retrying:
```bash
sudo mariadb -e "PURGE BINARY LOGS BEFORE NOW();"
sudo truncate -s 0 /var/log/mysql/slow.log
```

**Import timeouts or deadlocks**
rockdbutil retries automatically. For large tables, parallel chunk imports that hit lock
contention are automatically retried sequentially. No manual intervention is needed.

**InnoDB optimization warnings**
These require `SUPER` privilege. See [Required Database Privileges](#required-database-privileges). Imports proceed without them.

**Selective restore finds no changed tables**
The binlog may have been purged, or `last_export_meta.txt` is missing. Run a fresh export
first to record a new baseline, then make your changes and run selective restore again.

**Selective restore re-imports the same tables every run**
This happens if the binlog baseline was not advanced after the previous restore - usually
caused by an interrupted run. Delete the stale metadata and re-export:
```bash
rm ~/database_operations/last_export_meta.txt
./rockdbutil.sh -e -db your_profile
```

**No tables detected in a `.sql.gz` dump**
The file must be a standard `mysqldump` or `mariadb-dump` output. sqlsplit looks for
`-- Table structure for table` markers. Dumps created with non-standard tools may not be supported.

Error logs for individual tables are saved to `~/database_operations/logs/`.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Test on Arch, Manjaro, Ubuntu, and Debian
4. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) for details.
