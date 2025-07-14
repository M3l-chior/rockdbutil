# rockdbutil

Fast, reliable MariaDB/MySQL import/export tool with parallel processing and automatic error recovery.

## Features

- Parallel processing for multiple tables
- Automatic retry logic with deadlock/timeout handling  
- Cross-platform support (Arch, Ubuntu, Manjaro)
- Buffer pool optimization for faster imports
- Comprehensive error logging and recovery

## Quick Start

```bash
# Download
wget https://raw.githubusercontent.com/M3l-chior/rockdbutil/main/rockdbutil.sh
chmod +x rockdbutil.sh

# Configure (edit the script top section)
vim rockdbutil.sh  # Set DB_NAME, DB_USER, DB_PASS

# Export database
./rockdbutil.sh -e

# Import database  
./rockdbutil.sh -i db_dump_mydb_20231201_143022.tar.gz
```

## Prerequisites

Required (install manually):
- MariaDB/MySQL client tools
- Basic utilities: tar, gzip, bc

Auto-installed by script:
- GNU Parallel

## Usage

```bash
./rockdbutil.sh -e                    # Export database - Retains all .sql files and archive
./rockdbutil.sh -i backup.tar.gz      # Import database - Extracts and retains temp files
./rockdbutil.sh -d -e                 # Export with auto-cleanup - Retains just the archive
./rockdbutil.sh -d -i backup.tar.gz   # Import with auto-cleanup - Retains just the archive
./rockdbutil.sh -c                    # Configure connection for single session
```

## Performance

**Tested Performance Comparison:**

**700MB Database (1,256 Tables):**
- **Before:** `mariadb -u user -ppassword database_name < file.sql` - 28min
- **After:** `./rockdbutil.sh -i database_name.tar.gz` - 13min
- **Improvement:** 54% faster

**Test System Specs:**
- **CPU:** 16 cores (14 parallel threads used)
- **RAM:** 15GB total
- **Storage:** NVMe SSD (Micron 2210, 512GB)
- **OS:** Manjaro Linux (Kernel 6.15.3)
- **Buffer Pool:** Auto-optimized from 1GB → 9GB during import

**Speed improvements from:**
- **Parallel table processing** (14 concurrent imports)
- **Automatic buffer pool optimization** (temporary 9x increase)
- **Intelligent retry logic** for lock conflicts and deadlocks
- **Optimized import settings** (disabled foreign key checks during import)
- **Fast NVMe storage** enabling high-throughput parallel I/O

**Expected Performance on Different Hardware:**
- **NVMe SSD + 8+ cores:** Similar or better results
- **SATA SSD + 4-8 cores:** 30-40% improvement expected
- **Traditional HDD:** 20-30% improvement (limited by disk I/O)
- **Low RAM systems:** Automatic buffer optimization still provides benefits

*Results vary based on database structure, table count, hardware specs, and system load.* 
*The script automatically adapts buffer pool size and thread count to your current system capabilities.*

## Configuration

Edit the top of `rockdbutil.sh`:

```bash
DB_NAME="your_database"
DB_USER="your_user" 
DB_PASS="your_password"
```

## Troubleshooting

**Cannot connect to database:** Check credentials and MariaDB service status  
**Missing parallel command:** Install with `sudo apt install parallel` or `sudo pacman -S parallel`  
**Import timeouts:** Script automatically retries failed imports and optimizes settings

Error logs are saved to `~/database_operations/logs/`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Test on multiple distributions  
4. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) for details.
