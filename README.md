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
./rockdbutil.sh -e                    # Export database
./rockdbutil.sh -i backup.tar.gz      # Import database
./rockdbutil.sh -d -e                 # Export with auto-cleanup
./rockdbutil.sh -c                    # Configure connection
```

## Performance

**Tested Performance Comparison:**

**700MB Database:**
- **Before:** `mariadb -u user -ppassword database_name < file.sql` - 28min
- **After:** `./rockdbutil.sh -i database_name.tar.gz` - 13min
- **Improvement:** 54% faster

**Speed improvements from:**
- Parallel table processing
- Automatic buffer pool optimization
- Optimized import settings

*All tests performed with 5GB buffer pool optimization*

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
