# rockdbutil

Fast, reliable MariaDB/MySQL import/export tool with parallel processing, large-table chunked imports, and automatic error recovery.

## Features

- Parallel processing for multiple tables
- Chunked parallel import for large tables (configurable threshold)
- Monolithic `.sql.gz` dump support via `sqlsplit.sh` - no manual pre-processing needed
- Automatic retry logic with deadlock/lock-timeout handling
- Cross-platform support (Arch, Manjaro, Ubuntu, Debian)
- Temporary InnoDB optimizations for faster imports (buffer pool, flush log, doublewrite, I/O capacity)
- Storage-type detection (NVMe / SATA SSD / HDD) for automatic I/O tuning
- Comprehensive error logging and per-table recovery
- Multiple named database profiles in a single config file

## Quick Start

```bash
# Download both scripts (keep them in the same directory)
wget https://raw.githubusercontent.com/M3l-chior/rockdbutil/main/rockdbutil.sh
wget https://raw.githubusercontent.com/M3l-chior/rockdbutil/main/sqlsplit.sh
chmod +x rockdbutil.sh sqlsplit.sh

# Setup (first time only - creates config file and directories)
./rockdbutil.sh --setup

# Configure - set your database credentials
vim ~/.config/rockdbutil.conf

# Export database (default profile)
./rockdbutil.sh -e

# Export using a named profile
./rockdbutil.sh -e -db production

# Import a rockdbutil export (default profile)
./rockdbutil.sh -i db_dump_mydb_20231201_143022.tar.gz

# Import a monolithic mysqldump / mariadb-dump (.sql.gz)
./rockdbutil.sh -i db_backup.sql.gz

# Import to a named profile
./rockdbutil.sh -i db_dump_mydb_20231201_143022.tar.gz -db staging
```

## Prerequisites

Required (install manually):
- MariaDB or MySQL client tools (`mariadb` / `mysql`, `mariadb-dump` / `mysqldump`)
- Basic utilities: `tar`, `gzip`, `bc`, `awk`

Auto-installed by the script if missing:
- GNU Parallel

## Usage

```bash
./rockdbutil.sh --setup                                   # Initial setup
./rockdbutil.sh -e                                        # Export default database
./rockdbutil.sh -e -db production                         # Export using named profile
./rockdbutil.sh -e -d                                     # Export with auto-cleanup
./rockdbutil.sh -i backup.tar.gz                          # Import rockdbutil export (default profile)
./rockdbutil.sh -i db_backup.sql.gz                       # Import monolithic dump (default profile)
./rockdbutil.sh -i backup.tar.gz -db staging              # Import to named profile
./rockdbutil.sh -i db_backup.sql.gz -db staging           # Import monolithic dump to named profile
./rockdbutil.sh -d -i backup.tar.gz                       # Import with auto-cleanup and optimization
./rockdbutil.sh -d -i backup.tar.gz -db production        # Auto-optimized import to named profile
./rockdbutil.sh --list-profiles                           # List all configured database profiles
./rockdbutil.sh --test-connection                         # Test default profile connection
./rockdbutil.sh --test-connection -db production          # Test named profile connection
```

### Flags

| Flag | Long form | Description |
|---|---|---|
| `-e` | `--export` | Export database to a compressed `.tar.gz` archive |
| `-i FILE` | `--import FILE` | Import from a `.tar.gz` (rockdbutil export) or `.sql.gz` (monolithic dump) |
| `-db PROFILE` | `--database PROFILE` | Use a named database profile (default: `default`) |
| `-d` | `--auto-cleanup` | Remove temporary files after completion; also enables buffer pool optimization non-interactively |
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

## Performance

**Tested Performance Comparison:**

**6.5GB Database - 700MB Compressed (1,256 Tables):**
- **Before:** `mariadb -u user -ppassword database_name < file.sql` - 28 min
- **After:** `./rockdbutil.sh -i database_name.tar.gz` - 9 min
- **Improvement:** 68% faster

**Test System Specs:**
- **CPU:** 16 cores (14 parallel threads used)
- **RAM:** 15GB total
- **Storage:** NVMe SSD (Micron 2210, 512GB)
- **OS:** Manjaro Linux (Kernel 6.15.3)
- **Buffer Pool:** Auto-optimized from 1GB -> 9GB during import - (based on current available ram)

**Speed improvements from:**
- **Parallel table processing** (auto-detected thread count)
- **Chunked parallel import** for large tables - multiple concurrent INSERT streams per table
- **Temporary InnoDB optimizations** - buffer pool increase, flush log, doublewrite, I/O capacity
- **Storage-type detection** - NVMe/SSD I/O capacity tuned automatically
- **Intelligent retry logic** - lock timeouts and deadlocks retried; parallel chunk conflicts fall back to sequential automatically
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
large_table_threshold_mb=300

# Number of parallel INSERT streams per large table
large_table_chunks=4

# Number of large tables imported concurrently.
# Total concurrent streams = max_concurrent_large_tables × large_table_chunks
# e.g. 2 × 4 = 8 concurrent streams
max_concurrent_large_tables=2

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

## Working Directories

All working files are written under `base_directory` (default: `~/database_operations/`):

| Path | Purpose |
|---|---|
| `~/database_operations/dumps/` | Per-table `.sql` files during export |
| `~/database_operations/restore/` | Per-table `.sql` files during import |
| `~/database_operations/logs/` | Per-table error logs |
| `~/database_operations/error_report.txt` | Summary of any failed imports |
| `~/database_operations/success_log.txt` | List of successfully imported tables |

## Required Database Privileges

The database user needs the following privileges:

- Standard operations: `SELECT`, `SHOW DATABASES`, `SHOW VIEW`, `LOCK TABLES`, `EVENT`, `TRIGGER`
- InnoDB optimizations (optional but recommended): `SUPER`

Grant `SUPER` with:
```sql
GRANT SUPER ON *.* TO 'your_user'@'localhost';
FLUSH PRIVILEGES;
```

If `SUPER` is not available, rockdbutil will skip the InnoDB optimizations and continue with a warning.

## Troubleshooting

**Cannot connect to database**
Check your credentials in `~/.config/rockdbutil.conf` and confirm the MariaDB/MySQL service is running.

**`sqlsplit.sh` not found**
Both `rockdbutil.sh` and `sqlsplit.sh` must be in the same directory. The script expects `sqlsplit.sh` at the same path as itself.

**`sqlsplit.sh` not executable**
```bash
chmod +x sqlsplit.sh
```

**Missing `parallel` command**
```bash
# Arch / Manjaro
sudo pacman -S parallel

# Ubuntu / Debian
sudo apt install parallel
```

**Import timeouts or deadlocks**
rockdbutil retries automatically. For large tables, parallel chunk imports that hit lock contention are automatically retried sequentially. No manual intervention is needed.

**InnoDB optimization warnings**
These require `SUPER` privilege. See [Required Database Privileges](#required-database-privileges). Imports proceed without them.

**No tables detected in a `.sql.gz` dump**
The file must be a standard `mysqldump` or `mariadb-dump` output. sqlsplit looks for `-- Table structure for table` markers. Dumps created with non-standard tools may not be supported.

Error logs for individual tables are saved to `~/database_operations/logs/`.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Test on Arch, Manjaro, Ubuntu, and Debian
4. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) for details.
