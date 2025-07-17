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

# Setup (first time only)
./rockdbutil.sh --setup

# Configure (edit the generated config file)
vim ~/.config/rockdbutil.conf  # Set your database credentials

# Export database (default profile)
./rockdbutil.sh -e

# Export specific database profile
./rockdbutil.sh -e -b production

# Import database (default profile)
./rockdbutil.sh -i db_dump_mydb_20231201_143022.tar.gz

# Import to specific database profile
./rockdbutil.sh -i db_dump_mydb_20231201_143022.tar.gz -b staging
```

## Prerequisites

Required (install manually):
- MariaDB/MySQL client tools
- Basic utilities: tar, gzip, bc

Auto-installed by script:
- GNU Parallel

## Usage

```bash
./rockdbutil.sh --setup                         # Initial setup (creates config and directories)
./rockdbutil.sh -e                              # Export default database
./rockdbutil.sh -e -b production                # Export specific database profile
./rockdbutil.sh -i backup.tar.gz                # Import to default database
./rockdbutil.sh -i backup.tar.gz -b staging     # Import to specific database profile
./rockdbutil.sh -d -e                           # Export with auto-cleanup
./rockdbutil.sh -d -i backup.tar.gz -b prod     # Import with auto-cleanup and optimization
./rockdbutil.sh --list-profiles                 # List available database profiles
./rockdbutil.sh --test-connection -b production # Test specific database connection
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

The configuration file is located at `~/.config/rockdbutil.conf` and supports multiple database profiles:

```bash
# Default database (used when no -b flag specified)
default_db_name=your_database
default_db_user=your_user
default_db_pass=your_password
default_db_host=localhost
default_db_port=3306

# Production database profile
production_db_name=prod_database
production_db_user=prod_user
production_db_pass=prod_password
production_db_host=prod.company.com
production_db_port=3306

# Staging database profile
staging_db_name=staging_db
staging_db_user=staging_user
staging_db_pass=staging_password
staging_db_host=staging.company.com

# Global settings
threads_override=0
auto_cleanup=false
base_directory=$HOME/database_operations
buffer_optimization=true
```

If auto_cleanup=true then no -d flag is required when running the script.

**Create config file:** `./rockdbutil.sh --setup`

## Multi-Database Usage

Configure multiple database profiles for different environments:

```bash
# First time setup
./rockdbutil.sh --setup

# Edit configuration file
vim ~/.config/rockdbutil.conf

# List available profiles
./rockdbutil.sh --list-profiles

# Test connections to different profiles if setup in the config
./rockdbutil.sh --test-connection -b production
./rockdbutil.sh --test-connection -b staging
./rockdbutil.sh --test-connection -b dev

# Use specific database profiles
./rockdbutil.sh -e -b production             # Export from production
./rockdbutil.sh -i backup.tar.gz -b staging  # Import to staging
./rockdbutil.sh -d -i backup.tar.gz -b dev   # Import to dev with optimization
```

**Profile naming:** Use the prefix before `_db_name` as your profile name
- `production_db_name=prod_db` = Profile: `production`
- `dev_db_name=local_db` = Profile: `dev`
- `client_a_db_name=alpha_db` = Profile: `client_a`

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
