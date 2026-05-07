#!/bin/bash

# ===============================================
# rockdbutil.sh  
# Fast, reliable MariaDB/MySQL import/export tool
# with parallel processing and automatic error recovery
# 
# Copyright (c) 2025 Melchior (M3l-chior)
# Repository: https://github.com/M3l-chior/rockdbutil
#
# License: MIT
# ===============================================

set -euo pipefail 

# === COLOR CODES ===
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' 

# === GLOBAL VARIABLES ===
THREAD_COUNT_OVERRIDE=""
THREAD_MODE="conservative"
MYSQL_HOST_PARAMS=""
MYSQL_PORT_PARAMS=""
AUTO_CLEANUP_CONFIG=""
SQLSPLIT_PATH="$(dirname "$(realpath "$0")")/sqlsplit.sh"
LARGE_TABLE_THRESHOLD_MB=300
LARGE_TABLE_CHUNKS=4
MAX_CONCURRENT_LARGE_TABLES=2
INNODB_FLUSH_LOG_OPT=true
INNODB_DOUBLEWRITE_OPT=true
INNODB_IO_CAPACITY_OPT=true
EXPORT_FORMAT="csv"
CSV_OUTFILE_DIR=""
CSV_LOAD_MODE=""

# === LOGGING FUNCTIONS ===
log_info() {
	echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
	echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
	echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
	echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_step() {
	echo -e "${PURPLE}[STEP]${NC} $1"
}

log_progress() {
	echo -e "${CYAN}[PROGRESS]${NC} $1"
}

format_duration() {
	local total_seconds="$1"
	local minutes=$(( total_seconds / 60 ))
	local seconds=$(( total_seconds % 60 ))
	printf "%dm %ds" "$minutes" "$seconds"
}

log_timed_success() {
	local label="$1"
	local start_ts="$2"
	local end_ts
	end_ts=$(date +%s)
	log_success "$label ($(format_duration $(( end_ts - start_ts ))))"
}

# === CONFIGURATION ===
CONFIG_DIR="$HOME/.config"
CONFIG_FILE="$CONFIG_DIR/rockdbutil.conf"

DB_NAME=""
DB_USER=""
DB_PASS=""
CURRENT_DB_PROFILE=""

BASE_DIR="$HOME/database_operations"
DUMP_DIR="$BASE_DIR/dumps"
EXTRACT_DIR="$BASE_DIR/restore"
ERROR_LOG_DIR="$BASE_DIR/logs"
ERROR_REPORT="$BASE_DIR/error_report.txt"
SUCCESS_LOG="$BASE_DIR/success_log.txt"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
ARCHIVE_NAME=""

# === UTILITY FUNCTIONS ===
detect_distro() {
	if [[ -f /etc/os-release ]]; then
		. /etc/os-release
		echo "$ID"
	elif [[ -f /etc/arch-release ]]; then
		echo "arch"
	elif [[ -f /etc/debian_version ]]; then
		echo "debian"
	else
		echo "unknown"
	fi
}

get_package_manager() {
	local distro=$(detect_distro)
	case $distro in
		arch|manjaro)
			echo "pacman"
			;;
		ubuntu|debian)
			echo "apt"
			;;
		*)
			log_error "Unsupported distribution: $distro"
			exit 1
			;;
	esac
}

install_package() {
	local package=$1
	local pkg_manager=$(get_package_manager)
	
	log_info "Installing $package using $pkg_manager.."
	
	case $pkg_manager in
		pacman)
			# Handle package names for Arch
			if [[ "$package" == "procps-ng" ]]; then
				sudo pacman -S --noconfirm procps-ng
			else
				sudo pacman -S --noconfirm "$package"
			fi
			;;
		apt)
			# Handle package name for Deb/Ubuntu
			if [[ "$package" == "procps-ng" ]]; then
				package="procps"
			fi
			sudo apt update && sudo apt install -y "$package"
			;;
	esac
	}

check_command() {
	local cmd=$1
	local package=${2:-$cmd}
	
	if ! command -v "$cmd" &> /dev/null; then
		log_warning "$cmd not found. Attempting to install $package.."
		install_package "$package"
		
		if ! command -v "$cmd" &> /dev/null; then
			log_error "Failed to install $package. Please install manually."
			exit 1
		fi
		log_success "$package installed successfully."
	fi
}

get_mysql_command() {
	if command -v mariadb &> /dev/null; then
		echo "mariadb"
	elif command -v mysql &> /dev/null; then
		echo "mysql"
	else
		log_error "Neither mariadb nor mysql client found."
		local pkg_manager=$(get_package_manager)
		case $pkg_manager in
			pacman)
				log_info "Try: sudo pacman -S mariadb-clients"
				;;
			apt)
				log_info "Try: sudo apt install mariadb-client"
				;;
		esac
		exit 1
	fi
}

get_mysqldump_command() {
	if command -v mariadb-dump &> /dev/null; then
		echo "mariadb-dump"
	elif command -v mysqldump &> /dev/null; then
		echo "mysqldump"
	else
		log_error "Neither mariadb-dump nor mysqldump found."
		exit 1
	fi
}

get_storage_type() {
	local data_dir
	data_dir=$("$(get_mysql_command)" -u "$DB_USER" -p"$DB_PASS" -sN \
		-e "SELECT @@datadir;" 2>/dev/null)
	data_dir="${data_dir:-/var/lib/mysql}"

	local device
	device=$(df "$data_dir" 2>/dev/null | awk 'NR==2 {print $1}' | sed 's|/dev/||' | sed 's/p[0-9]*$//')

	local rotational_file="/sys/block/${device}/queue/rotational"
	if [[ ! -f "$rotational_file" ]]; then
		echo "unknown"
		return
	fi

	local rotational
	rotational=$(cat "$rotational_file" 2>/dev/null)

	if [[ "$rotational" == "0" ]]; then
		# Distinguish NVMe from SATA SSD by device name prefix
		if [[ "$device" == nvme* ]]; then
			echo "nvme"
		else
			echo "ssd"
		fi
	else
		echo "hdd"
	fi
}

# === CONFIGURATION FILE FUNCTIONS ===
create_default_config() {
	log_step "Creating default configuration file.."
	
	mkdir -p "$CONFIG_DIR"
	
	cat > "$CONFIG_FILE" << 'EOF'
# rockdbutil Configuration File
# Format: profile_setting=value

# Default database configuration (used when no -db flag specified) - profle name default
default_db_name=your_database
default_db_user=your_user
default_db_pass=your_password
default_db_host=localhost
default_db_port=3306

# Example additional database profile - profle name produciton
# production_db_name=prod_database
# production_db_user=prod_user
# production_db_pass=prod_password
# production_db_host=prod.example.com
# production_db_port=3306
#
# will be used as follows:
# rockdbutil -i dbdump.tar.gz -db production
# rockdbutil -i dbdump.sql.gz -db production
# rockdbutil -e -db production

# Global settings
threads_override=0
thread_mode=max
auto_cleanup=false
base_directory=$HOME/database_operations
buffer_optimization=true
log_level=info

# Large table chunked import settings
# Tables larger than this threshold (MB) are split into parallel import streams
large_table_threshold_mb=300
# Number of parallel streams per large table
large_table_chunks=4

# Number of large tables imported concurrently - each runs large_table_chunks parallel streams
# Total concurrent streams = max_concurrent_large_tables × large_table_chunks (e.g. 2 × 4 = 8)
max_concurrent_large_tables=2

# InnoDB import optimizations (applied temporarily during import, restored after)
# innodb_flush_log_at_trx_commit=2 flushes redo log to OS cache instead of disk per commit
# Significant throughput gain during controlled imports - safe to re-run from dump if interrupted
# Set to false for production databases where crash safety during import is required
innodb_flush_log_opt=true

# innodb_doublewrite=0 disables the doublewrite buffer during import - halves InnoDB write amplification
# Safe for controlled imports since you can re-run from the dump if interrupted
# Set to false if you require crash safety during import
innodb_doublewrite_opt=true

# innodb_io_capacity / innodb_io_capacity_max - tuned automatically based on detected storage type
# NVMe: 2000/8000  SSD: 1000/4000  HDD: left at server default (no change applied)
# Set to false to disable and leave server defaults in place
innodb_io_capacity_opt=true

# If auto_cleanup is false, it will retain the temp files unless if you supply the -d flag.
# if it is true, it will always remove the temp files.

# Export format (csv is default - significantly faster than sql at scale)
# Use --sql flag at runtime to force SQL export for a single run instead of changing this.
# csv  - exports schema as .schema.sql + data as .csv via SELECT INTO OUTFILE (default)
# sql  - exports full per-table .sql files via mysqldump (legacy behaviour)
export_format=csv

# Directory used by MariaDB server for SELECT INTO OUTFILE during CSV export.
# Must be writable by the MariaDB process. Defaults to base_directory/csv_export.
# If secure_file_priv is set on the server, this path must be within that restricted path.
csv_outfile_dir=
EOF

	chmod 600 "$CONFIG_FILE"
	log_success "Configuration file created: $CONFIG_FILE"
	log_info "Please edit the configuration file to set your database credentials"
	log_info "Run: vim $CONFIG_FILE or nano $CONFIG_FILE"
}

load_database_config() {
	local profile="${1:-default}"
	
	if [[ ! -f "$CONFIG_FILE" ]]; then
		log_error "Config file not found: $CONFIG_FILE"
		log_info "Run: $0 --setup to create the configuration file"
		exit 1
	fi
	
	CURRENT_DB_PROFILE="$profile"
	
	# Load profile-specific settings
	DB_NAME=$(grep "^${profile}_db_name=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')
	DB_USER=$(grep "^${profile}_db_user=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')
	DB_PASS=$(grep "^${profile}_db_pass=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')
	local DB_HOST=$(grep "^${profile}_db_host=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')
	local DB_PORT=$(grep "^${profile}_db_port=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')
	
	if [[ -z "$DB_NAME" || -z "$DB_USER" || -z "$DB_PASS" ]]; then
		log_error "Incomplete database configuration for profile: $profile"
		log_info "Required: ${profile}_db_name, ${profile}_db_user, ${profile}_db_pass"
		exit 1
	fi
	
	local threads_override=$(grep "^threads_override=" "$CONFIG_FILE" | cut -d'=' -f2 | tr -d '"')
	local auto_cleanup_config=$(grep "^auto_cleanup=" "$CONFIG_FILE" | cut -d'=' -f2 | tr -d '"')
	local base_dir_config=$(grep "^base_directory=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')
	
	if [[ -n "$threads_override" && "$threads_override" != "0" ]]; then
		THREAD_COUNT_OVERRIDE="$threads_override"
	fi

	local thread_mode=$(grep "^thread_mode=" "$CONFIG_FILE" | cut -d'=' -f2 | tr -d '"')
	if [[ -n "$thread_mode" ]]; then
		THREAD_MODE="$thread_mode"
	else
		THREAD_MODE="conservative"
	fi

	AUTO_CLEANUP_CONFIG="$auto_cleanup_config"

	local large_table_threshold=$(grep "^large_table_threshold_mb=" "$CONFIG_FILE" | cut -d'=' -f2 | tr -d '"')
	local large_table_chunks=$(grep "^large_table_chunks=" "$CONFIG_FILE" | cut -d'=' -f2 | tr -d '"')

	if [[ -n "$large_table_threshold" && "$large_table_threshold" -gt 0 ]]; then
		LARGE_TABLE_THRESHOLD_MB="$large_table_threshold"
	fi

	if [[ -n "$large_table_chunks" && "$large_table_chunks" -gt 0 ]]; then
		LARGE_TABLE_CHUNKS="$large_table_chunks"
	fi

	local max_concurrent_large=$(grep "^max_concurrent_large_tables=" "$CONFIG_FILE" | cut -d'=' -f2 | tr -d '"')
	if [[ -n "$max_concurrent_large" && "$max_concurrent_large" -gt 0 ]]; then
		MAX_CONCURRENT_LARGE_TABLES="$max_concurrent_large"
	fi

	local flush_log_opt=$(grep "^innodb_flush_log_opt=" "$CONFIG_FILE" | cut -d'=' -f2 | tr -d '"')
	if [[ -n "$flush_log_opt" ]]; then
		INNODB_FLUSH_LOG_OPT="$flush_log_opt"
	fi

	local doublewrite_opt=$(grep "^innodb_doublewrite_opt=" "$CONFIG_FILE" | cut -d'=' -f2 | tr -d '"')
	if [[ -n "$doublewrite_opt" ]]; then
		INNODB_DOUBLEWRITE_OPT="$doublewrite_opt"
	fi

	local io_capacity_opt=$(grep "^innodb_io_capacity_opt=" "$CONFIG_FILE" | cut -d'=' -f2 | tr -d '"')
	if [[ -n "$io_capacity_opt" ]]; then
		INNODB_IO_CAPACITY_OPT="$io_capacity_opt"
	fi

	if [[ -n "$base_dir_config" && "$base_dir_config" != "" ]]; then
		BASE_DIR="${base_dir_config/#\$HOME/$HOME}"
		DUMP_DIR="$BASE_DIR/dumps"
		EXTRACT_DIR="$BASE_DIR/restore"
		ERROR_LOG_DIR="$BASE_DIR/logs"
		ERROR_REPORT="$BASE_DIR/error_report.txt"
		SUCCESS_LOG="$BASE_DIR/success_log.txt"
	fi

	local export_format_config
	export_format_config=$(grep "^export_format=" "$CONFIG_FILE" | cut -d'=' -f2 | tr -d '"' | tr -d "'" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' || true)
	if [[ "$export_format_config" == "sql" ]]; then
		EXPORT_FORMAT="sql"
	else
		EXPORT_FORMAT="csv"
	fi

	local csv_outfile_dir_config
	csv_outfile_dir_config=$(grep "^csv_outfile_dir=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"' | tr -d "'" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' || true)
	if [[ -n "$csv_outfile_dir_config" ]]; then
		CSV_OUTFILE_DIR="${csv_outfile_dir_config/#\$HOME/$HOME}"
	else
		CSV_OUTFILE_DIR="$BASE_DIR/csv_export"
	fi
	
	if [[ -n "$DB_HOST" && "$DB_HOST" != "localhost" ]]; then
		MYSQL_HOST_PARAMS="-h $DB_HOST"
	fi
	if [[ -n "$DB_PORT" && "$DB_PORT" != "3306" ]]; then
		MYSQL_PORT_PARAMS="-P $DB_PORT"
	fi
	
	log_info "Loaded database profile: $profile ($DB_NAME)"
}

list_database_profiles() {
	if [[ ! -f "$CONFIG_FILE" ]]; then
		log_error "Config file not found: $CONFIG_FILE"
		return 1
	fi
	
	log_info "Available database profiles:"
	
	local profiles=$(grep "_db_name=" "$CONFIG_FILE" | sed 's/_db_name=.*//' | sort | uniq)
	
	for profile in $profiles; do
		local db_name=$(grep "^${profile}_db_name=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')
		local db_user=$(grep "^${profile}_db_user=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')
		local db_host=$(grep "^${profile}_db_host=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')
		
		if [[ "$profile" == "default" ]]; then
			echo -e "  ${GREEN}$profile${NC} (default) - $db_name @ ${db_host:-localhost} (user: $db_user)"
		else
			echo -e "  ${CYAN}$profile${NC} - $db_name @ ${db_host:-localhost} (user: $db_user)"
		fi
	done
}

setup_rockdbutil() {
	log_step "Setting up rockdbutil.."
	
	if [[ ! -f "$CONFIG_FILE" ]]; then
		create_default_config
	else
		log_info "Config file already exists: $CONFIG_FILE"
	fi
	
	setup_directories
	
	check_command "parallel"
	check_command "bc"
	check_command "gzip"
	check_command "tar" 
	check_command "pgrep" "procps-ng"
	local mysql_cmd=$(get_mysql_command)
	local mysqldump_cmd=$(get_mysqldump_command)
	
	log_success "rockdbutil setup completed successfully"
	echo
	echo -e "${WHITE}Next steps:${NC}"
	echo "1. Edit configuration: vim $CONFIG_FILE or nano $CONFIG_FILE"
	echo "2. Set your database credentials"
	echo "3. Test connection: $0 --test-connection"
	echo "4. Export database: $0 -e"
	echo "5. Import database: $0 -i backup.tar.gz"
	echo "If no profile is supplied (no -db flag), it will use the default profile in the config"
	echo
	echo -e "${WHITE}Multi-database usage:${NC}"
	echo "• List profiles: $0 --list-profiles"
	echo "• Use specific profile: $0 -db production -e"
}

test_db_connection() {
	local mysql_cmd=$(get_mysql_command)
	
	log_info "Testing database connection.."
	log_info "Profile: $CURRENT_DB_PROFILE | Database: $DB_NAME | User: $DB_USER"
	
	if ! "$mysql_cmd" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS -u "$DB_USER" -p"$DB_PASS" -e "SELECT 1;" &> /dev/null; then
		log_error "Cannot connect to database. Please check credentials and server status"
		log_info "Profile: $CURRENT_DB_PROFILE"
		log_info "Database: $DB_NAME"
		log_info "User: $DB_USER"
		log_info "Host: ${MYSQL_HOST_PARAMS:-localhost}"
		log_info "Port: ${MYSQL_PORT_PARAMS:-3306}"
		exit 1
	fi
	log_success "Database connection successful"
}

get_thread_count() {
	if [[ -n "$THREAD_COUNT_OVERRIDE" && "$THREAD_COUNT_OVERRIDE" -gt 0 ]]; then
		echo "$THREAD_COUNT_OVERRIDE"
		return
	fi
	
	local total_threads
	if command -v nproc &> /dev/null; then
		total_threads=$(nproc)
	elif command -v lscpu &> /dev/null; then
		total_threads=$(lscpu | grep '^CPU(s):' | awk '{print $2}')
	else
		total_threads=4
	fi
	
	local threads
	if [[ "$THREAD_MODE" == "max" ]]; then
		threads=$total_threads
	else
		threads=$((total_threads - 2))
		if [[ $threads -lt 1 ]]; then
			threads=1
		fi
	fi
	
	echo "$threads"
}

get_optimal_buffer_size() {
	local total_ram_gb=$(free -g | awk '/^Mem:/{print $2}')
	local current_buffer_gb=$($(get_mysql_command) -u "$DB_USER" -p"$DB_PASS" -e "SELECT ROUND(@@innodb_buffer_pool_size/1024/1024/1024, 2);" 2>/dev/null | tail -n +2)

	# Aggressive import-optimized sizing (temporary during import)
	local suggested_buffer_gb

	if [[ $total_ram_gb -ge 64 ]]; then
		# 64GB+ systems: ~50% (32GB+)
		suggested_buffer_gb=$((total_ram_gb * 50 / 100))
	elif [[ $total_ram_gb -ge 32 ]]; then
		# 32-63GB systems: ~60% (19-38GB)
		suggested_buffer_gb=$((total_ram_gb * 60 / 100))
	elif [[ $total_ram_gb -ge 16 ]]; then
		# 16-31GB systems: ~65% (10-20GB)
		suggested_buffer_gb=$((total_ram_gb * 65 / 100))
	elif [[ $total_ram_gb -ge 8 ]]; then
		# 8-15GB systems: ~70% (5-10GB)
		suggested_buffer_gb=$((total_ram_gb * 70 / 100))
	elif [[ $total_ram_gb -ge 4 ]]; then
		# 4-7GB systems: ~75% (3-5GB)
		suggested_buffer_gb=$((total_ram_gb * 75 / 100))
	else
		# <4GB systems: 1GB
		suggested_buffer_gb=1
	fi

	local available_gb=$(free -g | awk '/^Mem:/{print $7}')
	local safe_max_gb=$((available_gb - 1))

	if [[ $suggested_buffer_gb -gt $safe_max_gb ]]; then
		suggested_buffer_gb=$safe_max_gb
	fi

	if [[ $suggested_buffer_gb -lt 1 ]]; then
		suggested_buffer_gb=1
	fi
	
	echo "$current_buffer_gb:$suggested_buffer_gb:$total_ram_gb"
}

apply_import_optimizations() {
	local target_buffer_gb="$1"
	local target_buffer_bytes=$((target_buffer_gb * 1024 * 1024 * 1024))

	local mysql_cmd=$(get_mysql_command)

	log_info "Applying temporary import optimizations (buffer pool, InnoDB settings)..."

	local current_lock_wait
	current_lock_wait=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -sN \
		-e "SELECT @@innodb_lock_wait_timeout;" 2>/dev/null)
	echo "$current_lock_wait" > "$BASE_DIR/.original_lock_wait_timeout"
	"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
		-e "SET GLOBAL innodb_lock_wait_timeout = 600;" 2>/dev/null || true

	# Bump max_allowed_packet to 1GB for the duration of import.
	# LOAD DATA LOCAL INFILE sends file data in packets - the default 16MB limit
	# silently truncates large CSV loads after the first packet boundary.
	local current_max_packet
	current_max_packet=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -sN \
		-e "SELECT @@GLOBAL.max_allowed_packet;" 2>/dev/null)
	echo "${current_max_packet:-16777216}" > "$BASE_DIR/.original_max_allowed_packet"
	if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
			-e "SET GLOBAL max_allowed_packet = 1073741824;" 2>/dev/null; then
		log_success "max_allowed_packet set to 1GB (large CSV load support)"
	else
		log_warning "Could not set max_allowed_packet - large CSV loads may truncate"
		rm -f "$BASE_DIR/.original_max_allowed_packet"
	fi

	local current_buffer_bytes
	current_buffer_bytes=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -sN \
		-e "SELECT @@innodb_buffer_pool_size;" 2>/dev/null)
	echo "$current_buffer_bytes" > "$BASE_DIR/.original_buffer_size"

	if ! "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
		-e "SET GLOBAL innodb_buffer_pool_size = $target_buffer_bytes;" 2>/dev/null; then
		log_warning "Failed to change buffer pool - user may need SUPER privilege"
		log_info "Grant with: GRANT SUPER ON *.* TO '$DB_USER'@'localhost'; FLUSH PRIVILEGES;"
		rm -f "$BASE_DIR/.original_buffer_size"
		return 1
	fi
	log_success "Buffer pool increased to ${target_buffer_gb}GB"

	if [[ "$INNODB_FLUSH_LOG_OPT" == "true" ]]; then
		local current_flush
		current_flush=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -sN \
			-e "SELECT @@innodb_flush_log_at_trx_commit;" 2>/dev/null)
		echo "$current_flush" > "$BASE_DIR/.original_flush_log"
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
			-e "SET GLOBAL innodb_flush_log_at_trx_commit = 2;" 2>/dev/null; then
			log_success "innodb_flush_log_at_trx_commit set to 2 (OS cache flush)"
		else
			log_warning "Could not set innodb_flush_log_at_trx_commit - skipping"
			rm -f "$BASE_DIR/.original_flush_log"
		fi
	fi

	if [[ "$INNODB_DOUBLEWRITE_OPT" == "true" ]]; then
		local current_doublewrite
		current_doublewrite=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -sN \
			-e "SELECT @@innodb_doublewrite;" 2>/dev/null)
		echo "$current_doublewrite" > "$BASE_DIR/.original_doublewrite"
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
			-e "SET GLOBAL innodb_doublewrite = 0;" 2>/dev/null; then
			log_success "innodb_doublewrite disabled (halves write amplification)"
		else
			log_warning "Could not set innodb_doublewrite - skipping"
			rm -f "$BASE_DIR/.original_doublewrite"
		fi
	fi

	if [[ "$INNODB_IO_CAPACITY_OPT" == "true" ]]; then
		local storage_type
		storage_type=$(get_storage_type)
		local io_capacity=0
		local io_capacity_max=0

		case "$storage_type" in
			nvme)
				io_capacity=2000
				io_capacity_max=8000
				;;
			ssd)
				io_capacity=1000
				io_capacity_max=4000
				;;
			hdd|unknown)
				log_info "Storage type: ${storage_type} - leaving innodb_io_capacity at server default"
				;;
		esac

		if [[ "$io_capacity" -gt 0 ]]; then
			local current_io_capacity
			current_io_capacity=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -sN \
				-e "SELECT @@innodb_io_capacity;" 2>/dev/null)
			local current_io_capacity_max
			current_io_capacity_max=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -sN \
				-e "SELECT @@innodb_io_capacity_max;" 2>/dev/null)
			echo "$current_io_capacity"     > "$BASE_DIR/.original_io_capacity"
			echo "$current_io_capacity_max" > "$BASE_DIR/.original_io_capacity_max"

			if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
				-e "SET GLOBAL innodb_io_capacity = $io_capacity; SET GLOBAL innodb_io_capacity_max = $io_capacity_max;" 2>/dev/null; then
				log_success "innodb_io_capacity set to $io_capacity/$io_capacity_max (storage: $storage_type)"
			else
				log_warning "Could not set innodb_io_capacity - skipping"
				rm -f "$BASE_DIR/.original_io_capacity" "$BASE_DIR/.original_io_capacity_max"
			fi
		fi
	fi

	# Suppress binlog writes during import - parallel INSERT streams would otherwise generate
	# several GB of binlog for data that already exists in the dump. The binlog position
	# recorded at export time is what matters for selective restore; import events are noise.
	# sql_log_bin is session-level - export an env flag so worker sessions (parallel, chunks)
	# also prepend SET SESSION sql_log_bin = 0 to every import they run.
	local current_sql_log_bin
	current_sql_log_bin=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -sN \
		-e "SELECT @@SESSION.sql_log_bin;" 2>/dev/null)
	echo "${current_sql_log_bin:-1}" > "$BASE_DIR/.original_sql_log_bin"

	if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
		-e "SET SESSION sql_log_bin = 0;" 2>/dev/null; then
		log_success "sql_log_bin disabled (binlog writes suppressed for import)"
		export ROCKDBUTIL_SUPPRESS_BINLOG=1
	else
		log_warning "Could not disable sql_log_bin - binlog will be written during import"
		log_info "Grant with: GRANT SUPER ON *.* TO '$DB_USER'@'localhost'; FLUSH PRIVILEGES;"
		rm -f "$BASE_DIR/.original_sql_log_bin"
	fi

	# Suppress slow query log during import - chunk imports are intentionally long-running
	# and would flood slow.log with noise that is not actionable.
	local current_slow_log
	current_slow_log=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -sN \
		-e "SELECT @@GLOBAL.slow_query_log;" 2>/dev/null)
	echo "${current_slow_log:-1}" > "$BASE_DIR/.original_slow_query_log"

	if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
		-e "SET GLOBAL slow_query_log = 0;" 2>/dev/null; then
		log_success "slow_query_log disabled for import duration"
	else
		log_warning "Could not disable slow_query_log - slow.log will grow during import"
		rm -f "$BASE_DIR/.original_slow_query_log"
	fi

	return 0
}

restore_import_optimizations() {
	local mysql_cmd=$(get_mysql_command)

	local buffer_file="$BASE_DIR/.original_buffer_size"
	if [[ -f "$buffer_file" ]]; then
		local original_bytes=$(cat "$buffer_file")
		local original_gb=$((original_bytes / 1024 / 1024 / 1024))
		log_info "Restoring original buffer pool to ${original_gb}GB"
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
			-e "SET GLOBAL innodb_buffer_pool_size = $original_bytes;" 2>/dev/null; then
			log_success "Buffer pool restored"
		else
			log_warning "Failed to restore buffer pool"
		fi
		rm -f "$buffer_file"
	fi

	local flush_file="$BASE_DIR/.original_flush_log"
	if [[ -f "$flush_file" ]]; then
		local original_flush=$(cat "$flush_file")
		log_info "Restoring innodb_flush_log_at_trx_commit to $original_flush"
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
			-e "SET GLOBAL innodb_flush_log_at_trx_commit = $original_flush;" 2>/dev/null; then
			log_success "innodb_flush_log_at_trx_commit restored"
		else
			log_warning "Failed to restore innodb_flush_log_at_trx_commit"
		fi
		rm -f "$flush_file"
	fi

	local lock_wait_file="$BASE_DIR/.original_lock_wait_timeout"
	if [[ -f "$lock_wait_file" ]]; then
		local original_lock_wait=$(cat "$lock_wait_file")
		log_info "Restoring innodb_lock_wait_timeout to $original_lock_wait"
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
			-e "SET GLOBAL innodb_lock_wait_timeout = $original_lock_wait;" 2>/dev/null; then
			log_success "innodb_lock_wait_timeout restored"
		else
			log_warning "Failed to restore innodb_lock_wait_timeout"
		fi
		rm -f "$lock_wait_file"
	fi

	local doublewrite_file="$BASE_DIR/.original_doublewrite"
	if [[ -f "$doublewrite_file" ]]; then
		local original_doublewrite=$(cat "$doublewrite_file")
		log_info "Restoring innodb_doublewrite to $original_doublewrite"
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
			-e "SET GLOBAL innodb_doublewrite = $original_doublewrite;" 2>/dev/null; then
			log_success "innodb_doublewrite restored"
		else
			log_warning "Failed to restore innodb_doublewrite"
		fi
		rm -f "$doublewrite_file"
	fi

	local io_capacity_file="$BASE_DIR/.original_io_capacity"
	local io_capacity_max_file="$BASE_DIR/.original_io_capacity_max"
	if [[ -f "$io_capacity_file" && -f "$io_capacity_max_file" ]]; then
		local original_io_capacity=$(cat "$io_capacity_file")
		local original_io_capacity_max=$(cat "$io_capacity_max_file")
		log_info "Restoring innodb_io_capacity to $original_io_capacity/$original_io_capacity_max"
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
			-e "SET GLOBAL innodb_io_capacity = $original_io_capacity; SET GLOBAL innodb_io_capacity_max = $original_io_capacity_max;" 2>/dev/null; then
			log_success "innodb_io_capacity restored"
		else
			log_warning "Failed to restore innodb_io_capacity"
		fi
		rm -f "$io_capacity_file" "$io_capacity_max_file"
	fi

	local sql_log_bin_file="$BASE_DIR/.original_sql_log_bin"
	if [[ -f "$sql_log_bin_file" ]]; then
		local original_sql_log_bin
		original_sql_log_bin=$(cat "$sql_log_bin_file")
		log_info "Restoring sql_log_bin to $original_sql_log_bin"
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
			-e "SET SESSION sql_log_bin = $original_sql_log_bin;" 2>/dev/null; then
			log_success "sql_log_bin restored"
		else
			log_warning "Failed to restore sql_log_bin"
		fi
		rm -f "$sql_log_bin_file"
		unset ROCKDBUTIL_SUPPRESS_BINLOG
	fi

	local slow_log_file="$BASE_DIR/.original_slow_query_log"
	if [[ -f "$slow_log_file" ]]; then
		local original_slow_log
		original_slow_log=$(cat "$slow_log_file")
		log_info "Restoring slow_query_log to $original_slow_log"
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
			-e "SET GLOBAL slow_query_log = $original_slow_log;" 2>/dev/null; then
			log_success "slow_query_log restored"
		else
			log_warning "Failed to restore slow_query_log"
		fi
		rm -f "$slow_log_file"
	fi

	local max_packet_file="$BASE_DIR/.original_max_allowed_packet"
	if [[ -f "$max_packet_file" ]]; then
		local original_max_packet
		original_max_packet=$(cat "$max_packet_file")
		log_info "Restoring max_allowed_packet to $original_max_packet"
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
			-e "SET GLOBAL max_allowed_packet = $original_max_packet;" 2>/dev/null; then
			log_success "max_allowed_packet restored"
		else
			log_warning "Failed to restore max_allowed_packet"
		fi
		rm -f "$max_packet_file"
	fi
}

setup_directories() {
	if [[ ! -d "$BASE_DIR" ]]; then
		mkdir -p "$BASE_DIR"
		log_success "Created database operations directory: $BASE_DIR"
	fi
	
	mkdir -p "$DUMP_DIR"
	mkdir -p "$EXTRACT_DIR"
	mkdir -p "$ERROR_LOG_DIR"
	[[ -n "$CSV_OUTFILE_DIR" ]] && mkdir -p "$CSV_OUTFILE_DIR"
}

# === CSV HELPER FUNCTIONS ===

# check_secure_file_priv - queries the server's secure_file_priv setting to determine
# which LOAD DATA / INTO OUTFILE mode is available.
#
# Sets global CSV_LOAD_MODE to one of:
#   server   - secure_file_priv is empty (unrestricted) or csv_outfile_dir is within the allowed path
#   local    - secure_file_priv is NULL (server-side disabled); fall back to LOCAL INFILE
#   disabled - neither path is viable; CSV export cannot proceed
#
# Also validates that csv_outfile_dir is within the secure_file_priv path when restricted.
check_secure_file_priv() {
	local mysql_cmd
	mysql_cmd=$(get_mysql_command)

	local sfp_value
	sfp_value=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
		-sN -e "SELECT @@GLOBAL.secure_file_priv;" 2>/dev/null)

	if [[ -z "$sfp_value" ]]; then
		log_info "secure_file_priv: unrestricted - server-side CSV export enabled"
		CSV_LOAD_MODE="server"
		return 0
	fi

	if [[ "$sfp_value" == "NULL" ]]; then
		log_warning "secure_file_priv=NULL - server-side SELECT INTO OUTFILE is disabled"
		local local_infile
		local_infile=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
			-sN -e "SELECT @@GLOBAL.local_infile;" 2>/dev/null)
		if [[ "$local_infile" == "1" ]]; then
			log_info "local_infile=ON - will use LOAD DATA LOCAL INFILE fallback"
			CSV_LOAD_MODE="local"
		else
			log_warning "local_infile=OFF and secure_file_priv=NULL - CSV export not available"
			log_info "Enable with: SET GLOBAL local_infile = 1;"
			log_info "Or remove secure_file_priv restriction in server config"
			CSV_LOAD_MODE="disabled"
		fi
		return 0
	fi

	# Specific path restriction - verify csv_outfile_dir is within the allowed path
	local sfp_real
	sfp_real=$(realpath "$sfp_value" 2>/dev/null || echo "$sfp_value")
	local csv_real
	csv_real=$(realpath "$CSV_OUTFILE_DIR" 2>/dev/null || echo "$CSV_OUTFILE_DIR")

	if [[ "$csv_real" == "$sfp_real"* ]]; then
		log_info "secure_file_priv=$sfp_value - csv_outfile_dir is within allowed path"
		CSV_LOAD_MODE="server"
	else
		log_warning "secure_file_priv=$sfp_value restricts outfile to that path"
		log_warning "csv_outfile_dir ($CSV_OUTFILE_DIR) is outside the allowed path"
		log_info "Update csv_outfile_dir in config to a path within: $sfp_value"

		local local_infile
		local_infile=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
			-sN -e "SELECT @@GLOBAL.local_infile;" 2>/dev/null)
		if [[ "$local_infile" == "1" ]]; then
			log_info "local_infile=ON - falling back to LOAD DATA LOCAL INFILE"
			CSV_LOAD_MODE="local"
		else
			CSV_LOAD_MODE="disabled"
		fi
	fi
}

# detect_blob_tables - queries information_schema for any columns with blob/binary types
# in the target database. Tables with these types cannot be safely exported as CSV and
# must fall back to SQL export.
#
# Outputs a newline-separated list of affected table names to stdout (empty if none found).
detect_blob_tables() {
	local mysql_cmd
	mysql_cmd=$(get_mysql_command)

	"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
		-sN -e "
			SELECT DISTINCT TABLE_NAME
			FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = '${DB_NAME}'
			AND DATA_TYPE IN ('blob','mediumblob','longblob','tinyblob','binary','varbinary')
			ORDER BY TABLE_NAME;
		" 2>/dev/null
}

# export_table_schema - exports DDL only for a single table using mysqldump --no-data.
# Produces tablename.schema.sql in DUMP_DIR.
#
# Args:
#   $1 - table name
#   $2 - mysqldump command (mariadb-dump or mysqldump)
export_table_schema() {
	local table="$1"
	local mysqldump_cmd="$2"

	"$mysqldump_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
		--single-transaction --skip-lock-tables --no-data \
		--skip-triggers \
		"$DB_NAME" "$table" > "$DUMP_DIR/${table}.schema.sql" 2>/dev/null
}

# export_table_csv - exports table data as CSV.
#
# Two modes depending on CSV_LOAD_MODE:
#   server - uses SELECT INTO OUTFILE (server writes file to CSV_OUTFILE_DIR, moved to DUMP_DIR)
#   local  - uses mysql --batch --silent to write tab-separated data client-side.
#            Tab-separated with \N for NULLs matches LOAD DATA LOCAL INFILE natively -
#            no awk conversion needed, which keeps export speed comparable to mysqldump.
#
# Args:
#   $1 - table name
#   $2 - mysql client command (mariadb or mysql)
export_table_csv() {
	local table="$1"
	local mysql_cmd="$2"
	local dest="$DUMP_DIR/${table}.csv"

	if [[ "$CSV_LOAD_MODE" == "local" ]]; then
		"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
			--batch --silent --skip-column-names \
			"$DB_NAME" -e "SELECT * FROM \`${table}\`;" 2>/dev/null > "$dest"
	else
		local outfile="$CSV_OUTFILE_DIR/${table}.csv"
		rm -f "$outfile"

		"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
			"$DB_NAME" -e "
				SELECT * FROM \`${table}\`
				INTO OUTFILE '${outfile}'
				FIELDS TERMINATED BY ','
				OPTIONALLY ENCLOSED BY '\"'
				ESCAPED BY '\\\\'
				LINES TERMINATED BY '\n';
			" 2>/dev/null

		if [[ ! -f "$outfile" ]]; then
			log_warning "CSV export produced no file for table: $table"
			return 1
		fi

		mv "$outfile" "$dest"
	fi

	if [[ ! -f "$dest" ]]; then
		log_warning "CSV export produced no file for table: $table"
		return 1
	fi
}

# === EXPORT FUNCTION ===
export_database() {
	local auto_cleanup="$1"

	log_step "Starting database export process.."

	check_command "parallel"
	check_command "tar"
	check_command "gzip"
	test_db_connection

	ARCHIVE_NAME="db_dump_${DB_NAME}_${TIMESTAMP}.tar.gz"

	local mysql_cmd
	mysql_cmd=$(get_mysql_command)
	local mysqldump_cmd
	mysqldump_cmd=$(get_mysqldump_command)

	setup_directories
	if [[ -d "$DUMP_DIR" ]] && [[ "$(ls -A "$DUMP_DIR" 2>/dev/null)" ]]; then
		log_warning "Dump directory contains files. Removing old files.."
		rm -rf "$DUMP_DIR"/*
	fi
	log_success "Using dump directory: $DUMP_DIR"

	log_info "Retrieving table list from database: $DB_NAME"
	local tables
	if ! tables=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
			$MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
			-e "SHOW TABLES IN \`$DB_NAME\`;" 2>/dev/null | tail -n +2); then
		log_error "Failed to retrieve table list from database: $DB_NAME"
		exit 1
	fi

	if [[ -z "$tables" ]]; then
		log_error "No tables found in database: $DB_NAME"
		exit 1
	fi

	local table_count
	table_count=$(echo "$tables" | wc -l)
	log_success "Found $table_count tables to export"

	local threads
	threads=$(get_thread_count)

	if [[ "$EXPORT_FORMAT" == "csv" ]]; then
		_export_database_csv "$tables" "$table_count" "$threads" "$mysql_cmd" "$mysqldump_cmd"
	else
		_export_database_sql "$tables" "$table_count" "$mysqldump_cmd"
	fi

	log_step "Exporting triggers and routines..."
	if "$mysqldump_cmd" -u "$DB_USER" -p"$DB_PASS" \
			$MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
			--single-transaction --skip-lock-tables \
			--no-data --no-create-info \
			--routines --triggers \
			"$DB_NAME" > "$DUMP_DIR/__routines_and_triggers.sql" 2>/dev/null; then
		local routine_count
		routine_count=$(grep -c "^CREATE.*PROCEDURE\|^CREATE.*FUNCTION\|^CREATE.*TRIGGER" \
			"$DUMP_DIR/__routines_and_triggers.sql" 2>/dev/null || true)
		if [[ "$routine_count" -gt 0 ]]; then
			log_success "Exported ${routine_count} routine/trigger definition(s)"
		else
			rm -f "$DUMP_DIR/__routines_and_triggers.sql"
			log_info "No triggers or routines found in $DB_NAME"
		fi
	else
		log_warning "Failed to export triggers and routines"
	fi

	log_step "Recording binlog position at export time.."
	local master_status
	master_status=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" \
		$MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
		-sN -e "SHOW MASTER STATUS;" 2>/dev/null | head -1)

	local active_format="$EXPORT_FORMAT"
	if [[ -n "$master_status" ]]; then
		local binlog_file binlog_position
		binlog_file=$(echo "$master_status" | awk '{print $1}')
		binlog_position=$(echo "$master_status" | awk '{print $2}')

		# sql_fallback_tables is populated by _export_database_csv when blob tables are detected.
		# Empty for SQL exports and for CSV exports with no blob columns.
		cat > "$DUMP_DIR/__export_meta.txt" <<EOF
binlog_file=${binlog_file}
binlog_position=${binlog_position}
export_timestamp=$(date +"%Y-%m-%d %H:%M:%S")
db_name=${DB_NAME}
rockdbutil_version=1.0
export_format=${active_format}
csv_load_mode=${CSV_LOAD_MODE}
sql_fallback_tables=${SQL_FALLBACK_TABLES:-}
EOF
		log_success "Binlog position recorded: ${binlog_file}:${binlog_position}"
	else
		log_warning "SHOW MASTER STATUS returned no data - binlog position not recorded"
		log_info "Selective restore auto mode will require --since datetime fallback"
	fi

	log_step "Creating compressed archive..."
	if tar -czf "$ARCHIVE_NAME" -C "$DUMP_DIR" . 2>/dev/null; then
		local archive_size
		archive_size=$(du -h "$ARCHIVE_NAME" | cut -f1)
		log_success "Archive created: $ARCHIVE_NAME ($archive_size)"
	else
		log_error "Failed to create archive"
		exit 1
	fi

	if [[ "$auto_cleanup" == "true" ]]; then
		rm -rf "$DUMP_DIR"/*
		rm -rf "$CSV_OUTFILE_DIR"/*
		rm -f "$ERROR_REPORT" "$SUCCESS_LOG"
		rm -rf "$ERROR_LOG_DIR"/*
		log_success "Temporary files automatically cleaned up"
	fi
}

# _export_database_sql - internal SQL export path (legacy behaviour, --sql flag).
# Exports each table as a full per-table .sql file via mysqldump.
#
# Args:
#   $1 - newline-separated table list
#   $2 - total table count
#   $3 - mysqldump command
_export_database_sql() {
	local tables="$1"
	local table_count="$2"
	local mysqldump_cmd="$3"
	local exported=0

	log_step "Exporting tables as SQL (legacy mode).."
	while IFS= read -r table; do
		[[ -z "$table" ]] && continue
		log_progress "Dumping table: $table"
		if "$mysqldump_cmd" -u "$DB_USER" -p"$DB_PASS" \
				$MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
				--single-transaction --skip-lock-tables \
				"$DB_NAME" "$table" > "$DUMP_DIR/$table.sql" 2>/dev/null; then
			exported=$((exported + 1))
		else
			log_warning "Failed to dump table: $table"
		fi
	done <<< "$tables"

	log_success "Successfully exported $exported/$table_count tables"
}

# _export_database_csv - internal CSV export path (default).
# Detects blob tables (SQL fallback), exports schema via mysqldump --no-data,
# exports data via SELECT INTO OUTFILE, all running in parallel.
#
# Args:
#   $1 - newline-separated table list
#   $2 - total table count
#   $3 - thread count
#   $4 - mysql client command
#   $5 - mysqldump command
_export_database_csv() {
	local tables="$1"
	local table_count="$2"
	local threads="$3"
	local mysql_cmd="$4"
	local mysqldump_cmd="$5"

	check_secure_file_priv

	if [[ "$CSV_LOAD_MODE" == "disabled" ]]; then
		log_warning "CSV export not available - falling back to SQL export for all tables"
		EXPORT_FORMAT="sql"
		_export_database_sql "$tables" "$table_count" "$mysqldump_cmd"
		return
	fi

	log_info "CSV export mode: $CSV_LOAD_MODE (csv_outfile_dir: $CSV_OUTFILE_DIR)"

	local blob_tables=""
	blob_tables=$(detect_blob_tables)

	SQL_FALLBACK_TABLES=""
	if [[ -n "$blob_tables" ]]; then
		local blob_count
		blob_count=$(echo "$blob_tables" | wc -l)
		log_warning "Found $blob_count table(s) with blob/binary columns - these will use SQL export:"
		while IFS= read -r bt; do
			log_warning "  • $bt"
		done <<< "$blob_tables"
		SQL_FALLBACK_TABLES=$(echo "$blob_tables" | tr '\n' ',' | sed 's/,$//')
	fi

	log_step "Exporting table schemas and CSV data in parallel (${threads} threads).."

	# Build a temp file list so parallel gets clean input without subshell issues.
	local table_list_file
	table_list_file=$(mktemp)
	echo "$tables" > "$table_list_file"

	# Export schema + CSV (or SQL fallback) for each table.
	# parallel runs export_single_table_csv which is exported below.
	export -f export_single_table_csv export_table_schema export_table_csv
	export -f log_info log_warning log_error log_step log_success log_progress
	export DB_USER DB_PASS DB_NAME MYSQL_HOST_PARAMS MYSQL_PORT_PARAMS
	export DUMP_DIR CSV_OUTFILE_DIR SQL_FALLBACK_TABLES CSV_LOAD_MODE
	export RED GREEN YELLOW BLUE PURPLE CYAN WHITE NC

	parallel -j "$threads" export_single_table_csv {} "$mysql_cmd" "$mysqldump_cmd" \
		:::: "$table_list_file" || true

	rm -f "$table_list_file"

	local csv_count schema_count sql_fallback_count
	csv_count=$(find "$DUMP_DIR" -maxdepth 1 -name "*.csv" | wc -l)
	schema_count=$(find "$DUMP_DIR" -maxdepth 1 -name "*.schema.sql" | wc -l)
	sql_fallback_count=$(find "$DUMP_DIR" -maxdepth 1 -name "*.sql" \
		! -name "*.schema.sql" ! -name "__*.sql" | wc -l)

	log_success "Schema files: $schema_count  CSV files: $csv_count  SQL fallback: $sql_fallback_count"
}

# export_single_table_csv - per-table worker called by parallel during CSV export.
# Routes the table to CSV (schema + data) or SQL fallback (blob tables).
# Exported so GNU Parallel subshells can inherit it.
#
# Args:
#   $1 - table name
#   $2 - mysql client command
#   $3 - mysqldump command
export_single_table_csv() {
	local table="$1"
	local mysql_cmd="$2"
	local mysqldump_cmd="$3"

	[[ -z "$table" ]] && return 0

	# Check if this table is in the SQL fallback list (blob/binary columns).
	if echo ",$SQL_FALLBACK_TABLES," | grep -q ",${table},"; then
		log_progress "SQL fallback (blob): $table"
		"$mysqldump_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
			--single-transaction --skip-lock-tables \
			"$DB_NAME" "$table" > "$DUMP_DIR/${table}.sql" 2>/dev/null
		return $?
	fi

	log_progress "Exporting: $table"

	export_table_schema "$table" "$mysqldump_cmd" || {
		echo "[WARNING] Schema export failed for table: $table" >&2
		return 1
	}

	export_table_csv "$table" "$mysql_cmd" || {
		echo "[WARNING] CSV export failed for table: $table" >&2
		return 1
	}
}

# === IMPORT WORKER FUNCTIONS ===
# Defined at top level so both import_database() and selective_restore() can use them,
# and so GNU Parallel worker subshells inherit them via export -f.

# chunk_import_file - imports a single large table using N parallel INSERT streams.
# Streams the .sql file through awk splitting INSERT rows into equal chunks,
# each chunk piped directly into mariadb. No intermediate chunk files written to disk.
# The CREATE TABLE / preamble block is imported once first, then row chunks in parallel.
chunk_import_file() {
	local sql_file="$1"
	local mysql_cmd="$2"
	local chunks="$3"
	local filename
	filename=$(basename "$sql_file")
	local error_log="$ERROR_LOG_DIR/${filename}.chunk.error"
	local table_name="${filename%.sql}"

	log_progress "Chunked import: $table_name (${chunks} parallel streams)"

	# Step 1: Import the schema block (everything before the first INSERT statement).
	# Stop at the LOCK TABLES line - the dump format has INSERT INTO `tbl` VALUES
	# on its own line followed by data rows starting with (, so it must not be included.
	awk '/^LOCK TABLES / { exit } { print }' "$sql_file" \
		| "$mysql_cmd" --batch --silent -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" 2>"$error_log"

	if [[ $? -ne 0 ]]; then
		echo "FAILED: $filename (schema phase)" >> "${ERROR_REPORT}.${table_name}"
		[[ -s "$error_log" ]] && sed 's/^/    /' "$error_log" >> "${ERROR_REPORT}.${table_name}"
		return 1
	fi

	# Step 2: Extract the INSERT header line and count total data rows for chunk division.
	local header_file="$ERROR_LOG_DIR/${filename}.header.tmp"
	grep -m 1 "^INSERT INTO" "$sql_file" > "$header_file" || true

	if [[ ! -s "$header_file" ]]; then
		rm -f "$header_file"
		echo "$filename" >> "$SUCCESS_LOG"
		return 0
	fi

	local total_rows
	local count_file="${sql_file%.sql}.rowcount"
	if [[ -f "$count_file" ]]; then
		total_rows=$(cat "$count_file")
	else
		total_rows=$(awk '/^\(/ { count++ } END { print count+0 }' "$sql_file" 2>/dev/null || true)
	fi
	total_rows="${total_rows:-0}"

	local rows_per_chunk=0
	if [[ "$total_rows" -gt 0 ]]; then
		rows_per_chunk=$(( (total_rows + chunks - 1) / chunks ))
	fi

	if [[ "$rows_per_chunk" -eq 0 ]]; then
		echo "FAILED: $filename (could not compute chunk size, total_rows=$total_rows)" >> "${ERROR_REPORT}.${table_name}"
		return 1
	fi

	# Step 3: Stream each chunk directly into mariadb in parallel.
	# Each chunk gets its own transaction wrapper to prevent autocommit-mode
	# row-by-row locking that causes deadlocks between parallel chunk streams.
	local chunk_pids=()
	local chunk_errors=()
	local chunk_idx=0

	while [[ $chunk_idx -lt $chunks ]]; do
		local chunk_start=$(( chunk_idx * rows_per_chunk + 1 ))
		local chunk_end=$(( chunk_start + rows_per_chunk - 1 ))
		echo "DEBUG chunk$chunk_idx: start=$chunk_start end=$chunk_end total=$total_rows rpc=$rows_per_chunk" >> "$ERROR_LOG_DIR/${filename}.debug"
		local chunk_error_log="$ERROR_LOG_DIR/${filename}.chunk${chunk_idx}.error"
		chunk_errors+=("$chunk_error_log")

		{
			[[ "${ROCKDBUTIL_SUPPRESS_BINLOG:-0}" == "1" ]] && echo "SET SESSION sql_log_bin = 0;"
			echo "SET foreign_key_checks = 0;"
			echo "SET unique_checks = 0;"
			echo "SET autocommit = 0;"
			echo "START TRANSACTION;"
			awk -v header_file="$header_file" \
				-v start="$chunk_start" \
				-v end="$chunk_end" \
				-v total_end="$total_rows" '
				BEGIN {
					if ((getline header < header_file) <= 0) header = ""
					close(header_file)
					start     = start + 0
					end       = end + 0
					total_end = total_end + 0
					row_num   = 0
					last_line = ""
					started   = 0
				}
				/^\(/ {
					row_num++
					if (row_num < start) next
					if (row_num > end) exit
					if (row_num == start) {
						print header
						started = 1
					}
					if (started && last_line != "") {
						if (last_line ~ /;[[:space:]]*$/) {
							print last_line
							print header
							last_line = ""
						} else {
							print last_line
						}
					}
					last_line = $0
				}
				END {
					if (started && last_line != "") {
						gsub(/[;,][[:space:]]*$/, "", last_line)
						print last_line ";"
					}
				}
			' "$sql_file"
			echo "COMMIT;"
		} | "$mysql_cmd" --batch --silent -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" 2>"$chunk_error_log" &

		chunk_pids+=($!)
		(( chunk_idx++ )) || true
	done

	# Wait for all parallel chunk imports to complete
	local chunk_failed=0
	local pid_idx=0
	while [[ $pid_idx -lt ${#chunk_pids[@]} ]]; do
		if ! wait "${chunk_pids[$pid_idx]}"; then
			chunk_failed=$(( chunk_failed + 1 ))
			cat "${chunk_errors[$pid_idx]}" >> "$error_log" 2>/dev/null || true
		fi
		(( pid_idx++ )) || true
	done

	local i=0
	while [[ $i -lt ${#chunk_errors[@]} ]]; do
		rm -f "${chunk_errors[$i]}"
		(( i++ )) || true
	done

	# If chunks failed due to lock contention, truncate and retry sequentially.
	# Parallel inserts on tables with unique/primary key indexes cause deadlocks -
	# sequential retry eliminates contention while preserving transaction-size benefit.
	if [[ $chunk_failed -gt 0 ]] && grep -q "Lock wait timeout\|Deadlock" "$error_log" 2>/dev/null; then
		log_warning "$table_name: lock contention on parallel chunks - truncating and retrying sequentially"
		"$mysql_cmd" --batch --silent -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" \
			-e "SET foreign_key_checks = 0; TRUNCATE TABLE \`${table_name}\`;" 2>/dev/null || true
		> "$error_log"
		chunk_failed=0
		chunk_idx=0

		while [[ $chunk_idx -lt $chunks ]]; do
			local chunk_start=$(( chunk_idx * rows_per_chunk + 1 ))
			local chunk_end=$(( chunk_start + rows_per_chunk - 1 ))
			local chunk_error_log="$ERROR_LOG_DIR/${filename}.chunk${chunk_idx}.error"

			{
				[[ "${ROCKDBUTIL_SUPPRESS_BINLOG:-0}" == "1" ]] && echo "SET SESSION sql_log_bin = 0;"
				echo "SET foreign_key_checks = 0;"
				echo "SET unique_checks = 0;"
				echo "SET autocommit = 0;"
				echo "START TRANSACTION;"
				awk -v header_file="$header_file" \
					-v start="$chunk_start" \
					-v end="$chunk_end" \
					-v total_end="$total_rows" '
					BEGIN {
						if ((getline header < header_file) <= 0) header = ""
						close(header_file)
						start     = start + 0
						end       = end + 0
						total_end = total_end + 0
						row_num   = 0
						last_line = ""
						started   = 0
					}
					/^\(/ {
						row_num++
						if (row_num < start) next
						if (row_num > end) exit
						if (row_num == start) {
							print header
							started = 1
						}
						if (started && last_line != "") {
							if (last_line ~ /;[[:space:]]*$/) {
								print last_line
								print header
								last_line = ""
							} else {
								print last_line
							}
						}
						last_line = $0
					}
					END {
						if (started && last_line != "") {
							gsub(/[;,][[:space:]]*$/, "", last_line)
							print last_line ";"
						}
					}
				' "$sql_file"
				echo "COMMIT;"
			} | "$mysql_cmd" --batch --silent -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" 2>"$chunk_error_log"

			if [[ $? -ne 0 ]]; then
				chunk_failed=$(( chunk_failed + 1 ))
				cat "$chunk_error_log" >> "$error_log" 2>/dev/null || true
			fi
			rm -f "$chunk_error_log"
			(( chunk_idx++ )) || true
		done

		if [[ $chunk_failed -eq 0 ]]; then
			log_success "$table_name: sequential retry succeeded"
		fi
	fi

	if [[ $chunk_failed -gt 0 ]]; then
		local debug_log="$ERROR_LOG_DIR/${filename}.chunk_debug.log"
		{
			echo "=== chunk error for $filename ==="
			echo "--- insert header ---"
			grep -m 1 "^INSERT INTO" "$sql_file" || true
			echo "--- mariadb error (last 30 lines) ---"
			tail -30 "$error_log"
		} > "$debug_log"
		echo "FAILED: $filename ($chunk_failed/$chunks chunks failed)" >> "${ERROR_REPORT}.${table_name}"
		[[ -s "$error_log" ]] && sed 's/^/    /' "$error_log" >> "${ERROR_REPORT}.${table_name}"
		return 1
	fi

	echo "$filename" >> "$SUCCESS_LOG"
	rm -f "$error_log" "$header_file"
	return 0
}

# import_single_file - imports a single small table file.
# Routes large tables (in __large_tables.txt manifest) to the deferred list instead,
# so they are handled by chunk_import_file after all small tables complete.
import_single_file() {
	local sql_file="$1"
	local filename
	filename=$(basename "$sql_file")
	local mysql_cmd="$2"
	local error_log="$ERROR_LOG_DIR/${filename}.error"
	local max_retries=3
	local retry=0

	local manifest="$EXTRACT_DIR/__large_tables.txt"
	local table_name="${filename%.sql}"
	if [[ -f "$manifest" ]] && grep -qx "$table_name" "$manifest"; then
		grep -qxF "$table_name" "$EXTRACT_DIR/__deferred_large.txt" 2>/dev/null || echo "$table_name" >> "$EXTRACT_DIR/__deferred_large.txt"
		return 0
	fi

	while [[ $retry -lt $max_retries ]]; do
		if {
			[[ "${ROCKDBUTIL_SUPPRESS_BINLOG:-0}" == "1" ]] && echo "SET SESSION sql_log_bin = 0;"
			cat "$sql_file"
		} | "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" 2>"$error_log"; then
			echo "$filename" >> "$SUCCESS_LOG"
			rm -f "$error_log"
			[[ $retry -gt 0 ]] && echo "SUCCESS: $filename (after $retry retries)" >&2
			return 0
		else
			((retry++))
			if grep -q "Lock wait timeout exceeded" "$error_log" && [[ $retry -lt $max_retries ]]; then
				echo "RETRY $retry/$max_retries: $filename (lock timeout)" >&2
				sleep $((retry * 2))
			elif grep -q "Deadlock found" "$error_log" && [[ $retry -lt $max_retries ]]; then
				echo "RETRY $retry/$max_retries: $filename (deadlock)" >&2
				sleep $((retry))
			else
				break
			fi
		fi
	done

	local retry_text=""
	[[ $retry -gt 0 ]] && retry_text=" (after $retry retries)"
	echo "FAILED: $filename$retry_text" >> "$ERROR_REPORT"
	if [[ -s "$error_log" ]]; then
		echo "  Error details:" >> "$ERROR_REPORT"
		sed 's/^/    /' "$error_log" >> "$ERROR_REPORT"
	fi
	echo "" >> "$ERROR_REPORT"
	return 1
}

# import_csv_file - imports a single table from a .csv file using LOAD DATA INFILE.
# Routes large tables (in __large_tables.txt manifest) to the deferred list instead,
# so they are handled by chunk_import_csv after all small tables complete.
#
# Respects CSV_LOAD_MODE: "server" uses LOAD DATA INFILE, "local" uses LOAD DATA LOCAL INFILE.
# Same retry logic as import_single_file for lock timeouts and deadlocks.
#
# Args:
#   $1 - csv file path
#   $2 - mysql client command
import_csv_file() {
	local csv_file="$1"
	local mysql_cmd="$2"
	local filename
	filename=$(basename "$csv_file")
	local table_name="${filename%.csv}"
	local error_log="$ERROR_LOG_DIR/${filename}.error"
	local max_retries=3
	local retry=0

	local manifest="$EXTRACT_DIR/__large_tables.txt"
	if [[ -f "$manifest" ]] && grep -qx "$table_name" "$manifest"; then
		local deferred_file="$EXTRACT_DIR/__deferred_large_csv.txt"
		local deferred_lock="$EXTRACT_DIR/__deferred_large_csv.lock"
		(
			flock -x 9
			grep -qxF "$table_name" "$deferred_file" 2>/dev/null \
				|| echo "$table_name" >> "$deferred_file"
		) 9>"$deferred_lock"
		return 0
	fi

	local load_keyword="LOAD DATA INFILE"
	[[ "$CSV_LOAD_MODE" == "local" ]] && load_keyword="LOAD DATA LOCAL INFILE"

	local field_terminator="','"
	[[ "$CSV_LOAD_MODE" == "local" ]] && field_terminator="'\\t'"

	local csv_abs
	csv_abs=$(realpath "$csv_file" 2>/dev/null || echo "$csv_file")

	while [[ $retry -lt $max_retries ]]; do
		local suppress_sql=""
		[[ "${ROCKDBUTIL_SUPPRESS_BINLOG:-0}" == "1" ]] && suppress_sql="SET SESSION sql_log_bin = 0;"

		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
				"$DB_NAME" 2>"$error_log" <<EOF
${suppress_sql}
SET foreign_key_checks = 0;
SET unique_checks = 0;
${load_keyword} '${csv_abs}'
INTO TABLE \`${table_name}\`
FIELDS TERMINATED BY ${field_terminator}
ESCAPED BY '\\\\'
LINES TERMINATED BY '\n';
EOF
		then
			echo "$filename" >> "$SUCCESS_LOG"
			rm -f "$error_log"
			[[ $retry -gt 0 ]] && echo "SUCCESS: $filename (after $retry retries)" >&2
			return 0
		else
			((retry++))
			if grep -q "Lock wait timeout exceeded" "$error_log" && [[ $retry -lt $max_retries ]]; then
				echo "RETRY $retry/$max_retries: $filename (lock timeout)" >&2
				sleep $((retry * 2))
			elif grep -q "Deadlock found" "$error_log" && [[ $retry -lt $max_retries ]]; then
				echo "RETRY $retry/$max_retries: $filename (deadlock)" >&2
				sleep $((retry))
			else
				break
			fi
		fi
	done

	local retry_text=""
	[[ $retry -gt 0 ]] && retry_text=" (after $retry retries)"
	echo "FAILED: $filename$retry_text" >> "$ERROR_REPORT"
	if [[ -s "$error_log" ]]; then
		echo "  Error details:" >> "$ERROR_REPORT"
		sed 's/^/    /' "$error_log" >> "$ERROR_REPORT"
	fi
	echo "" >> "$ERROR_REPORT"
	return 1
}

# chunk_import_csv - imports a large table CSV using N parallel LOAD DATA INFILE streams.
# Splits the CSV by line count into N equal chunks using `split`, loads each chunk in
# parallel, cleans up chunk files on success.
#
# Falls back to sequential single-chunk load on lock contention (truncates first).
#
# Args:
#   $1 - csv file path
#   $2 - mysql client command
#   $3 - number of chunks
chunk_import_csv() {
	local csv_file="$1"
	local mysql_cmd="$2"
	local chunks="$3"
	local filename
	filename=$(basename "$csv_file")
	local table_name="${filename%.csv}"
	local error_log="$ERROR_LOG_DIR/${filename}.chunk.error"
	local csv_abs
	csv_abs=$(realpath "$csv_file" 2>/dev/null || echo "$csv_file")

	log_progress "Chunked CSV import: $table_name (${chunks} parallel streams)"

	local total_lines
	total_lines=$(wc -l < "$csv_file" 2>/dev/null || echo 0)

	if [[ "$total_lines" -eq 0 ]]; then
		log_warning "$table_name: CSV file is empty - skipping"
		echo "$filename" >> "$SUCCESS_LOG"
		return 0
	fi

	local load_keyword="LOAD DATA INFILE"
	[[ "$CSV_LOAD_MODE" == "local" ]] && load_keyword="LOAD DATA LOCAL INFILE"

	local field_terminator="','"
	[[ "$CSV_LOAD_MODE" == "local" ]] && field_terminator="'\\t'"

	local suppress_sql=""
	[[ "${ROCKDBUTIL_SUPPRESS_BINLOG:-0}" == "1" ]] && suppress_sql="SET SESSION sql_log_bin = 0;"

	if [[ "$CSV_LOAD_MODE" == "local" ]]; then
		log_progress "Large CSV import: $table_name (single-stream local mode)"

		"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
			"$DB_NAME" \
			-e "ALTER TABLE \`${table_name}\` DISABLE KEYS;" 2>/dev/null || true

		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
				"$DB_NAME" 2>"$error_log" <<EOF
${suppress_sql}
SET foreign_key_checks = 0;
SET unique_checks = 0;
${load_keyword} '${csv_abs}'
INTO TABLE \`${table_name}\`
FIELDS TERMINATED BY ${field_terminator}
ESCAPED BY '\\\\'
LINES TERMINATED BY '\n';
EOF
		then
			"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
				"$DB_NAME" \
				-e "ALTER TABLE \`${table_name}\` ENABLE KEYS;" 2>/dev/null || true
			echo "$filename" >> "$SUCCESS_LOG"
			rm -f "$error_log"
			return 0
		fi

		"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
			"$DB_NAME" \
			-e "ALTER TABLE \`${table_name}\` ENABLE KEYS;" 2>/dev/null || true
		echo "FAILED: $filename (large CSV local import failed)" >> "${ERROR_REPORT}.${table_name}"
		[[ -s "$error_log" ]] && sed 's/^/    /' "$error_log" >> "${ERROR_REPORT}.${table_name}"
		return 1
	fi

	local lines_per_chunk=$(( (total_lines + chunks - 1) / chunks ))

	local chunk_prefix="$EXTRACT_DIR/${table_name}_csvchunk_"
	split -l "$lines_per_chunk" "$csv_file" "$chunk_prefix"

	# Disable secondary key maintenance for the duration of bulk load.
	# DISABLE KEYS eliminates index lock contention between parallel chunk streams
	# and defers the index rebuild to a single sorted pass after all data is loaded,
	# which is substantially faster than per-row index updates during LOAD DATA.
	"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
		"$DB_NAME" \
		-e "ALTER TABLE \`${table_name}\` DISABLE KEYS;" 2>/dev/null || true

	local chunk_pids=()
	local chunk_files=()

	while IFS= read -r -d '' chunk_file; do
		chunk_files+=("$chunk_file")
		local chunk_abs
		chunk_abs=$(realpath "$chunk_file" 2>/dev/null || echo "$chunk_file")
		local chunk_error_log="${chunk_file}.error"

		"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
				"$DB_NAME" 2>"$chunk_error_log" <<EOF &
${suppress_sql}
SET foreign_key_checks = 0;
SET unique_checks = 0;
${load_keyword} '${chunk_abs}'
INTO TABLE \`${table_name}\`
FIELDS TERMINATED BY ${field_terminator}
ESCAPED BY '\\\\'
LINES TERMINATED BY '\n';
EOF
		chunk_pids+=($!)
	done < <(find "$EXTRACT_DIR" -maxdepth 1 -name "${table_name}_csvchunk_*" -print0 | sort -z)

	local chunk_failed=0
	local pid_idx=0
	while [[ $pid_idx -lt ${#chunk_pids[@]} ]]; do
		if ! wait "${chunk_pids[$pid_idx]}"; then
			chunk_failed=$(( chunk_failed + 1 ))
			cat "${chunk_files[$pid_idx]}.error" >> "$error_log" 2>/dev/null || true
		fi
		rm -f "${chunk_files[$pid_idx]}.error"
		(( pid_idx++ )) || true
	done

	# Lock contention on parallel chunk loads - truncate and retry sequentially.
	# DISABLE KEYS is already in effect so the sequential retry has no index contention.
	if [[ $chunk_failed -gt 0 ]] && grep -q "Lock wait timeout\|Deadlock" "$error_log" 2>/dev/null; then
		log_warning "$table_name: lock contention on CSV chunks - truncating and retrying sequentially"
		"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
			"$DB_NAME" \
			-e "SET foreign_key_checks = 0; TRUNCATE TABLE \`${table_name}\`;" 2>/dev/null || true
		> "$error_log"
		chunk_failed=0

		# Re-split into fresh chunk files and load one at a time.
		# rm -f only runs on a successful load - a failed chunk is left on disk
		# so the error log and chunk count remain consistent for diagnostics.
		split -l "$lines_per_chunk" "$csv_file" "$chunk_prefix"
		while IFS= read -r -d '' retry_chunk; do
			local retry_abs
			retry_abs=$(realpath "$retry_chunk" 2>/dev/null || echo "$retry_chunk")
				"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
						"$DB_NAME" 2>>"$error_log" <<EOF
${suppress_sql}
SET foreign_key_checks = 0;
SET unique_checks = 0;
${load_keyword} '${retry_abs}'
INTO TABLE \`${table_name}\`
FIELDS TERMINATED BY ${field_terminator}
ESCAPED BY '\\\\'
LINES TERMINATED BY '\n';
EOF
			if [[ $? -ne 0 ]]; then
				chunk_failed=$(( chunk_failed + 1 ))
			else
				rm -f "$retry_chunk"
			fi
		done < <(find "$EXTRACT_DIR" -maxdepth 1 -name "${table_name}_csvchunk_*" -print0 | sort -z)
	fi

	# Rebuild secondary indexes in a single sorted pass now that all data is loaded.
	# Runs regardless of outcome so the table is left in a consistent state.
	"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
		"$DB_NAME" \
		-e "ALTER TABLE \`${table_name}\` ENABLE KEYS;" 2>/dev/null || true

	# Clean up any remaining first-pass chunk files (retry chunks already removed above).
	local cf
	for cf in "${chunk_files[@]}"; do
		rm -f "$cf"
	done

	if [[ $chunk_failed -gt 0 ]]; then
		echo "FAILED: $filename (chunked CSV import failed)" >> "${ERROR_REPORT}.${table_name}"
		[[ -s "$error_log" ]] && sed 's/^/    /' "$error_log" >> "${ERROR_REPORT}.${table_name}"
		return 1
	fi

	echo "$filename" >> "$SUCCESS_LOG"
	rm -f "$error_log"
	return 0
}

# Export worker functions and all globals they depend on.
# Called once at script load time - both import_database() and selective_restore()
# rely on these being available in GNU Parallel worker subshells.
register_worker_exports() {
	export -f import_single_file chunk_import_file import_csv_file chunk_import_csv
	export -f log_progress log_info log_success log_warning log_error log_step
	export DB_USER DB_PASS DB_NAME MYSQL_HOST_PARAMS MYSQL_PORT_PARAMS
	export ERROR_LOG_DIR ERROR_REPORT SUCCESS_LOG EXTRACT_DIR LARGE_TABLE_CHUNKS
	export INNODB_FLUSH_LOG_OPT INNODB_DOUBLEWRITE_OPT INNODB_IO_CAPACITY_OPT MAX_CONCURRENT_LARGE_TABLES
	export ROCKDBUTIL_SUPPRESS_BINLOG CSV_LOAD_MODE
	export RED GREEN YELLOW BLUE PURPLE CYAN WHITE NC
}

# === IMPORT FILE RESOLVER ===
# Detects whether the supplied file is a monolithic .sql.gz or a rockdbutil .tar.gz.
# Monolithic .sql.gz imports are first split into per-table SQL files so the parallel
# SQL import and large-table chunking pipeline can be used.
resolve_import_file() {
	local archive_file="$1"

	if [[ "$archive_file" == *.sql.gz ]]; then
		IMPORT_TYPE="split_sql"
		return
	fi

	IMPORT_TYPE="archive"
}

# === IMPORT FUNCTION ===
import_database() {
	local archive_file="$1"
	local auto_cleanup="$2"

	local cleanup_buffer=false
	local import_start_time=$(date +%s)
	trap 'if [[ "${cleanup_buffer:-false}" == "true" ]]; then restore_import_optimizations; fi' EXIT

	if [[ -z "$archive_file" ]]; then
		log_error "Archive file not specified"
		show_usage
		exit 1
	fi

	if [[ ! -f "$archive_file" ]]; then
		log_error "Archive file not found: $archive_file"
		exit 1
	fi

	log_step "Starting database import process.."

	check_command "parallel"
	check_command "bc"
	test_db_connection

	# Clean any stale optimization state files from previous interrupted runs.
	rm -f "$BASE_DIR"/.original_*

	local buffer_info
	buffer_info=$(get_optimal_buffer_size)
	local current_buffer_gb
	current_buffer_gb=$(echo "$buffer_info" | cut -d: -f1)
	local suggested_buffer_gb
	suggested_buffer_gb=$(echo "$buffer_info" | cut -d: -f2)
	local total_ram_gb
	total_ram_gb=$(echo "$buffer_info" | cut -d: -f3)

	echo -e "${WHITE}System Memory Status:${NC}"
	echo "Total RAM: ${total_ram_gb}GB"
	echo "Available: $(free -h | awk '/^Mem:/{print $7}')"
	echo "Current Buffer Pool: ${current_buffer_gb}GB"

	local use_buffer_optimization=false
	local available_gb
	available_gb=$(free -g | awk '/^Mem:/{print $7}')
	local safe_max_gb=$((available_gb - 2))
	local original_suggested=$((total_ram_gb * 70 / 100))

	if [[ $original_suggested -gt $safe_max_gb ]]; then
		log_info "Optimization available: ${suggested_buffer_gb}GB (reduced from ${original_suggested}GB for safety)"
	else
		log_info "Optimization available: ${suggested_buffer_gb}GB"
	fi

	if [[ $(echo "$suggested_buffer_gb > $current_buffer_gb" | bc 2>/dev/null) == "1" ]]; then
		log_info "Will use: ${suggested_buffer_gb}GB (optimized)"
	else
		log_info "Will use: ${current_buffer_gb}GB (current is already optimal)"
	fi

	if [[ $(echo "$suggested_buffer_gb > $current_buffer_gb" | bc 2>/dev/null) == "1" ]]; then
		log_info "Buffer optimization will be applied: ${current_buffer_gb}GB → ${suggested_buffer_gb}GB"

		if [[ "$auto_cleanup" == "true" ]]; then
			if apply_import_optimizations "$suggested_buffer_gb"; then
				use_buffer_optimization=true
				cleanup_buffer=true
			fi
		else
			read -p "$(echo -e "${YELLOW}Temporarily increase buffer pool to ${suggested_buffer_gb}GB for faster import? [Y/n]:${NC} ")" -n 1 -r
			echo
			if [[ ! $REPLY =~ ^[Nn]$ ]]; then
				if apply_import_optimizations "$suggested_buffer_gb"; then
					use_buffer_optimization=true
					cleanup_buffer=true
				fi
			fi
		fi
	fi

	local mysql_cmd
	mysql_cmd=$(get_mysql_command)
	local threads
	threads=$(get_thread_count)
	log_info "Using $threads parallel threads for import"

	setup_directories
	if [[ -d "$EXTRACT_DIR" ]] && [[ "$(ls -A "$EXTRACT_DIR" 2>/dev/null)" ]]; then
		log_warning "Extract directory contains files. Removing old files.."
		rm -rf "$EXTRACT_DIR"/*
	fi
	rm -rf "$ERROR_LOG_DIR"/*
	log_success "Using extraction directory: $EXTRACT_DIR"

	IMPORT_TYPE=""
	resolve_import_file "$archive_file"

	if [[ "$IMPORT_TYPE" == "split_sql" ]]; then
		log_step "Detected monolithic SQL dump (.sql.gz) - invoking sqlsplit.."

		if [[ ! -f "$SQLSPLIT_PATH" ]]; then
			log_error "sqlsplit.sh not found at: $SQLSPLIT_PATH"
			log_info "Place sqlsplit.sh in the same directory as rockdbutil.sh"
			exit 1
		fi

		if [[ ! -x "$SQLSPLIT_PATH" ]]; then
			log_error "sqlsplit.sh is not executable: $SQLSPLIT_PATH"
			log_info "Run: chmod +x $SQLSPLIT_PATH"
			exit 1
		fi

		bash "$SQLSPLIT_PATH" "$archive_file" --direct-to-dir "$EXTRACT_DIR" --db-name "$DB_NAME" --threshold "$LARGE_TABLE_THRESHOLD_MB"
		IMPORT_TYPE="split"
	elif [[ "$IMPORT_TYPE" == "archive" ]]; then
		log_step "Extracting archive: $archive_file"
		if tar -xzf "$archive_file" -C "$EXTRACT_DIR" 2>/dev/null; then
			local file_count
			file_count=$(find "$EXTRACT_DIR" -name "*.sql" | wc -l)
			log_success "Extracted $file_count SQL files"
		else
			log_error "Failed to extract archive"
			exit 1
		fi
	else
		local file_count
		file_count=$(find "$EXTRACT_DIR" -maxdepth 1 \( -name "*.sql" -o -name "*.csv" \) ! -name "__*.sql" | wc -l)
		log_success "Split complete - $file_count files ready in extract directory"
	fi

	if [[ -f "$EXTRACT_DIR/__export_meta.txt" ]]; then
		cp "$EXTRACT_DIR/__export_meta.txt" "$BASE_DIR/last_export_meta.txt"
		log_info "Export metadata saved to: $BASE_DIR/last_export_meta.txt"
	fi

	local archive_export_format="sql"
	local archive_csv_load_mode="server"
	if [[ -f "$EXTRACT_DIR/__export_meta.txt" ]]; then
		local meta_format
		meta_format=$(grep "^export_format=" "$EXTRACT_DIR/__export_meta.txt" | cut -d'=' -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' || true)
		[[ "$meta_format" == "csv" ]] && archive_export_format="csv"

		local meta_load_mode
		meta_load_mode=$(grep "^csv_load_mode=" "$EXTRACT_DIR/__export_meta.txt" | cut -d'=' -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' || true)
		[[ -n "$meta_load_mode" ]] && archive_csv_load_mode="$meta_load_mode"
	fi

	> "$ERROR_REPORT"
	> "$SUCCESS_LOG"

	register_worker_exports

	if [[ "$archive_export_format" == "csv" ]]; then
		CSV_LOAD_MODE="$archive_csv_load_mode"
		export CSV_LOAD_MODE
		log_info "CSV archive detected (load_mode: $CSV_LOAD_MODE) - using three-phase import"
		_import_database_csv "$mysql_cmd" "$threads" "$auto_cleanup" "$use_buffer_optimization"
	else
		_import_database_sql "$mysql_cmd" "$threads" "$auto_cleanup" "$use_buffer_optimization"
	fi

	local import_end_time
	import_end_time=$(date +%s)
	local import_duration=$(( import_end_time - import_start_time ))
	local import_minutes=$(( import_duration / 60 ))
	local import_seconds=$(( import_duration % 60 ))

	local import_success_flag="$BASE_DIR/.import_success"
	local import_count_file="$BASE_DIR/.import_count"
	local import_success=false
	local total_imported=0
	[[ -f "$import_success_flag" ]] && import_success=true
	[[ -f "$import_count_file" ]] && total_imported=$(cat "$import_count_file" 2>/dev/null || echo "0")
	rm -f "$import_success_flag" "$import_count_file"

	echo
	if [[ "$import_success" == "true" ]]; then
		echo -e "${GREEN}╔══════════════════════════════════════════╗${NC}"
		echo -e "${GREEN}║       Import completed successfully!     ║${NC}"
		echo -e "${GREEN}╚══════════════════════════════════════════╝${NC}"
		echo -e "  Database : ${WHITE}$DB_NAME${NC}"
		echo -e "  Tables   : ${WHITE}${total_imported}${NC}"
		echo -e "  Format   : ${WHITE}${archive_export_format}${NC}"
		echo -e "  Duration : ${WHITE}${import_minutes}m ${import_seconds}s${NC}"
		echo
	else
		echo -e "${RED}╔══════════════════════════════════════════╗${NC}"
		echo -e "${RED}║         Import completed with errors     ║${NC}"
		echo -e "${RED}╚══════════════════════════════════════════╝${NC}"
		echo -e "  Database : ${WHITE}$DB_NAME${NC}"
		echo -e "  Check    : ${WHITE}$ERROR_REPORT${NC}"
		echo
		exit 1
	fi
}

# _import_database_sql - SQL import path (legacy .tar.gz and .sql.gz archives).
# Unchanged from Phase 1 behaviour - parallel import_single_file, deferred large
# table chunking via chunk_import_file, retry on failure.
#
# Args:
#   $1 - mysql client command
#   $2 - thread count
#   $3 - auto_cleanup flag
#   $4 - use_buffer_optimization flag
_import_database_sql() {
	local mysql_cmd="$1"
	local threads="$2"
	local auto_cleanup="$3"
	local use_buffer_optimization="$4"

	local sql_files=("$EXTRACT_DIR"/*.sql)
	if [[ ! -e "${sql_files[0]}" ]]; then
		log_error "No SQL files found in archive"
		exit 1
	fi

	log_step "Importing SQL files into database: $DB_NAME"
	log_info "This may take some time depending on database size.."

	# Import sequences first - shared across tables, conflict under parallel import.
	local sequences_file="$EXTRACT_DIR/__sequences.sql"
	if [[ -f "$sequences_file" && -s "$sequences_file" ]]; then
		log_step "Importing database sequences (pre-import step).."
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
				"$DB_NAME" < "$sequences_file" 2>"$ERROR_LOG_DIR/__sequences.error"; then
			log_success "Sequences imported successfully"
			rm -f "$ERROR_LOG_DIR/__sequences.error"
		else
			log_warning "Sequence import had errors (may be harmless if sequences already exist)"
			grep -v "^$" "$ERROR_LOG_DIR/__sequences.error" | head -5 >&2 || true
		fi
	fi

	local total_files
	total_files=$(find "$EXTRACT_DIR" -name "*.sql" ! -name "__*.sql" | wc -l)

	_monitor_import_progress "$total_files" "import_single_file" &
	local monitor_pid=$!

	local sql_file_list=()
	while IFS= read -r -d '' f; do
		sql_file_list+=("$f")
	done < <(find "$EXTRACT_DIR" -maxdepth 1 -name "*.sql" ! -name "__*.sql" -print0)

	parallel -j "$threads" import_single_file {} "$mysql_cmd" ::: "${sql_file_list[@]}" || true
	kill "$monitor_pid" 2>/dev/null || true
	wait "$monitor_pid" 2>/dev/null || true

	local deferred_file="$EXTRACT_DIR/__deferred_large.txt"
	if [[ -f "$deferred_file" && -s "$deferred_file" ]]; then
		log_info "Pre-computing row counts for large tables.."
		while IFS= read -r table_name; do
			local sql_file="$EXTRACT_DIR/${table_name}.sql"
			awk '/^\(/ { count++ } END { print count+0 }' "$sql_file" \
				> "$EXTRACT_DIR/${table_name}.rowcount" &
		done < "$deferred_file"
		wait
		log_info "Row counts ready"

		log_step "Importing large tables (${MAX_CONCURRENT_LARGE_TABLES} concurrent, ${LARGE_TABLE_CHUNKS} streams per table).."
		local large_pids=()
		local running=0

		while IFS= read -r table_name; do
			local sql_file="$EXTRACT_DIR/${table_name}.sql"
			local file_size_mb=$(( $(stat -c%s "$sql_file" 2>/dev/null || echo 0) / 1024 / 1024 ))
			log_progress "Large table: $table_name (${file_size_mb}MB)"
			chunk_import_file "$sql_file" "$mysql_cmd" "$LARGE_TABLE_CHUNKS" &
			large_pids+=($!)
			running=$(( running + 1 ))

			if [[ $running -ge $MAX_CONCURRENT_LARGE_TABLES ]]; then
				wait "${large_pids[0]}" || true
				large_pids=("${large_pids[@]:1}")
				running=$(( running - 1 ))
			fi
		done < "$deferred_file"

		local p
		for p in "${large_pids[@]}"; do
			wait "$p" || true
		done
	fi

	local success_count
	success_count=$(wc -l < "$SUCCESS_LOG" 2>/dev/null || echo "0")
	local import_success=false

	if grep -q "^FAILED:" "$ERROR_REPORT" 2>/dev/null; then
		local total_count
		total_count=$(find "$EXTRACT_DIR" -name "*.sql" ! -name "__*.sql" | wc -l)
		local failed_count=$(( total_count - success_count ))
		log_warning "$failed_count out of $total_count imports failed"
		log_info "Will retry failed imports sequentially.."
		if retry_failed_imports "$mysql_cmd"; then
			log_success "Failed imports successfully retried"
			import_success=true
		else
			log_error "Some imports still failed after retry attempts"
		fi
	else
		log_success "All $success_count SQL files imported successfully"
		import_success=true
	fi

	local final_count
	final_count=$(wc -l < "$SUCCESS_LOG" 2>/dev/null || echo "0")
	echo "$final_count" > "$BASE_DIR/.import_count"

	if [[ "$use_buffer_optimization" == "true" ]]; then
		restore_import_optimizations
	fi

	if [[ "$auto_cleanup" == "true" && "$import_success" == "true" ]]; then
		rm -rf "$EXTRACT_DIR"/*
		rm -f "$ERROR_REPORT" "$SUCCESS_LOG"
		rm -rf "$ERROR_LOG_DIR"/*
		log_success "Temporary files automatically cleaned up"
	fi

	[[ "$import_success" == "true" ]] && touch "$BASE_DIR/.import_success"
}

# _import_database_csv - three-phase CSV import path.
#
# Phase 1 (sequential): Import all *.schema.sql files - DDL only, fast, FK checks off.
#                       SQL fallback tables (blob columns) use their full .sql files here.
# Phase 2 (parallel):   LOAD DATA INFILE for all *.csv files via import_csv_file.
#                       Large tables deferred to chunk_import_csv.
# Phase 3 (sequential): Routines, triggers, sequences - unchanged from SQL path.
#
# Args:
#   $1 - mysql client command
#   $2 - thread count
#   $3 - auto_cleanup flag
#   $4 - use_buffer_optimization flag
_import_database_csv() {
	local mysql_cmd="$1"
	local threads="$2"
	local auto_cleanup="$3"
	local use_buffer_optimization="$4"

	# -- Phase 1: Schema --
	log_step "Phase 1/3 - Importing table schemas.."
	local schema_phase_start
	schema_phase_start=$(date +%s)

	local schema_files=()
	while IFS= read -r -d '' f; do
		schema_files+=("$f")
	done < <(find "$EXTRACT_DIR" -maxdepth 1 -name "*.schema.sql" -print0)

	# SQL fallback files (blob tables) - full .sql files, not just schema
	local sql_fallback_files=()
	while IFS= read -r -d '' f; do
		sql_fallback_files+=("$f")
	done < <(find "$EXTRACT_DIR" -maxdepth 1 -name "*.sql" \
		! -name "*.schema.sql" ! -name "__*.sql" -print0)

	if [[ ${#schema_files[@]} -eq 0 && ${#sql_fallback_files[@]} -eq 0 ]]; then
		log_error "No schema files found in CSV archive"
		exit 1
	fi

	local schema_errors=0
	local schema_file
	for schema_file in "${schema_files[@]}"; do
		local schema_name
		schema_name=$(basename "$schema_file" .schema.sql)
		if ! {
			echo "SET foreign_key_checks = 0;"
			cat "$schema_file"
		} | "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
				"$DB_NAME" 2>"$ERROR_LOG_DIR/${schema_name}.schema.error"; then
			log_warning "Schema import failed for: $schema_name"
			schema_errors=$(( schema_errors + 1 ))
		else
			rm -f "$ERROR_LOG_DIR/${schema_name}.schema.error"
		fi
	done

	local schema_count=${#schema_files[@]}
	local imported_schemas=$(( schema_count - schema_errors ))
	log_timed_success "Schemas imported: $imported_schemas/$schema_count" "$schema_phase_start"

	# -- Phase 1b: SQL fallback tables (blob columns) --
	if [[ ${#sql_fallback_files[@]} -gt 0 ]]; then
		log_step "Phase 1b/3 - Importing SQL fallback tables (blob/binary columns).."
		local fallback_phase_start
		fallback_phase_start=$(date +%s)

		# Import sequences first if present
		local sequences_file="$EXTRACT_DIR/__sequences.sql"
		if [[ -f "$sequences_file" && -s "$sequences_file" ]]; then
			if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
					"$DB_NAME" < "$sequences_file" 2>"$ERROR_LOG_DIR/__sequences.error"; then
				log_success "Sequences imported successfully"
				rm -f "$ERROR_LOG_DIR/__sequences.error"
			else
				log_warning "Sequence import had errors (may be harmless if sequences already exist)"
				grep -v "^$" "$ERROR_LOG_DIR/__sequences.error" | head -5 >&2 || true
			fi
		fi

		parallel -j "$threads" import_single_file {} "$mysql_cmd" ::: "${sql_fallback_files[@]}" || true
		local fallback_success
		fallback_success=$(wc -l < "$SUCCESS_LOG" 2>/dev/null || echo "0")
		log_timed_success "SQL fallback tables imported: $fallback_success/${#sql_fallback_files[@]}" "$fallback_phase_start"
	fi

	# -- Phase 2: CSV data --
	log_step "Phase 2/3 - Loading CSV data (${threads} parallel streams).."
	local csv_phase_start
	csv_phase_start=$(date +%s)

	local csv_files=()
	while IFS= read -r -d '' f; do
		csv_files+=("$f")
	done < <(find "$EXTRACT_DIR" -maxdepth 1 -name "*.csv" -print0)

	if [[ ${#csv_files[@]} -eq 0 ]]; then
		log_warning "No CSV files found in archive - skipping data load phase"
	else
		local total_csv=${#csv_files[@]}

		_monitor_import_progress "$total_csv" "import_csv_file" &
		local monitor_pid=$!

		parallel -j "$threads" import_csv_file {} "$mysql_cmd" ::: "${csv_files[@]}" || true
		kill "$monitor_pid" 2>/dev/null || true
		wait "$monitor_pid" 2>/dev/null || true

		# Large CSV tables deferred by import_csv_file into __deferred_large_csv.txt
		local deferred_csv_file="$EXTRACT_DIR/__deferred_large_csv.txt"
		if [[ -f "$deferred_csv_file" && -s "$deferred_csv_file" ]]; then
			log_step "Importing large CSV tables (${MAX_CONCURRENT_LARGE_TABLES} concurrent, ${LARGE_TABLE_CHUNKS} streams per table).."
			local large_pids=()
			local running=0

			while IFS= read -r table_name; do
				local csv_file="$EXTRACT_DIR/${table_name}.csv"
				local file_size_mb=$(( $(stat -c%s "$csv_file" 2>/dev/null || echo 0) / 1024 / 1024 ))
				log_progress "Large CSV table: $table_name (${file_size_mb}MB)"
				chunk_import_csv "$csv_file" "$mysql_cmd" "$LARGE_TABLE_CHUNKS" &
				large_pids+=($!)
				running=$(( running + 1 ))

				if [[ $running -ge $MAX_CONCURRENT_LARGE_TABLES ]]; then
					wait "${large_pids[0]}" || true
					large_pids=("${large_pids[@]:1}")
					running=$(( running - 1 ))
				fi
			done < "$deferred_csv_file"

			local p
			for p in "${large_pids[@]}"; do
				wait "$p" || true
			done
		fi

			local csv_success_count
			csv_success_count=$(wc -l < "$SUCCESS_LOG" 2>/dev/null || echo "0")
			log_timed_success "CSV tables loaded: $csv_success_count/$total_csv" "$csv_phase_start"
	fi

	# -- Phase 3: Routines, triggers, sequences --
	log_step "Phase 3/3 - Importing routines and triggers.."
	local routines_phase_start
	routines_phase_start=$(date +%s)

	local routines_file="$EXTRACT_DIR/__routines_and_triggers.sql"
	if [[ -f "$routines_file" && -s "$routines_file" ]]; then
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" $MYSQL_HOST_PARAMS $MYSQL_PORT_PARAMS \
				"$DB_NAME" < "$routines_file" 2>"$ERROR_LOG_DIR/__routines.error"; then
			log_success "Routines and triggers imported successfully"
			rm -f "$ERROR_LOG_DIR/__routines.error"
		else
			log_warning "Routines/triggers import had errors"
			grep -v "^$" "$ERROR_LOG_DIR/__routines.error" | head -5 >&2 || true
		fi
	else
		log_info "No routines or triggers file found - skipping"
	fi
	log_timed_success "Phase 3 complete" "$routines_phase_start"

	local import_success=false
	if grep -q "^FAILED:" "$ERROR_REPORT" 2>/dev/null; then
		local success_count
		success_count=$(wc -l < "$SUCCESS_LOG" 2>/dev/null || echo "0")
		log_warning "Some imports failed - retrying sequentially.."
		if retry_failed_imports "$mysql_cmd"; then
			log_success "Failed imports successfully retried"
			import_success=true
		else
			log_error "Some imports still failed after retry attempts"
		fi
	else
		import_success=true
	fi

	local final_count
	final_count=$(wc -l < "$SUCCESS_LOG" 2>/dev/null || echo "0")
	echo "$final_count" > "$BASE_DIR/.import_count"

	if [[ "$use_buffer_optimization" == "true" ]]; then
		restore_import_optimizations
	fi

	if [[ "$auto_cleanup" == "true" && "$import_success" == "true" ]]; then
		rm -rf "$EXTRACT_DIR"/*
		rm -f "$ERROR_REPORT" "$SUCCESS_LOG"
		rm -rf "$ERROR_LOG_DIR"/*
		log_success "Temporary files automatically cleaned up"
	fi

	[[ "$import_success" == "true" ]] && touch "$BASE_DIR/.import_success"
}

# _monitor_import_progress - background progress reporter for parallel import phases.
# Polls SUCCESS_LOG every 3 seconds and logs incremental progress.
# Terminates when no parallel worker matching the given pattern is running.
#
# Args:
#   $1 - total file count to import
#   $2 - pattern to match against pgrep (e.g. "import_single_file" or "import_csv_file")
_monitor_import_progress() {
	local total="$1"
	local worker_pattern="$2"
	local start_time
	start_time=$(date +%s)
	local last_count=0
	local last_log_time=$start_time

	while true; do
		if [[ -f "$SUCCESS_LOG" ]]; then
			local completed
			completed=$(wc -l < "$SUCCESS_LOG" 2>/dev/null || echo "0")
			local current_time
			current_time=$(date +%s)
			local time_since_last_log=$(( current_time - last_log_time ))

			if [[ $completed -gt $last_count && $completed -gt 0 ]]; then
				local elapsed=$(( current_time - start_time ))
				local minutes=$(( elapsed / 60 ))
				local seconds=$(( elapsed % 60 ))
				log_progress "Imported $completed/$total tables (${minutes}m ${seconds}s elapsed)"
				last_count=$completed
				last_log_time=$current_time
			elif [[ $time_since_last_log -ge 10 && $completed -gt 0 ]]; then
				local remaining=$(( total - completed ))
				log_progress "Still working: $completed/$total complete, $remaining remaining"
				last_log_time=$current_time
			fi
		fi
		sleep 3

		if ! pgrep -f "parallel.*${worker_pattern}" > /dev/null; then
			break
		fi
	done
}

# Temporarily increase buffer pool (session only)
test_larger_buffer_pool() {
	local mysql_cmd=$(get_mysql_command)
	
	log_step "Testing larger buffer pool (temporary change)..."
	
	echo -e "${WHITE}Current buffer pool:${NC}"
	"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -e "SELECT ROUND(@@innodb_buffer_pool_size/1024/1024/1024, 3) AS current_buffer_pool_GB;"
	
	echo -e "${WHITE}Available system memory:${NC}"
	free -h | grep Mem
	
	log_warning "Buffer pool size requires MariaDB restart to change"
	log_info "Let's create a simple config change..."
	
	local config_file=""
	local possible_configs=(
		"/etc/mysql/mariadb.conf.d/50-server.cnf"
		"/etc/mysql/mysql.conf.d/mysqld.cnf" 
		"/etc/mysql/my.cnf"
		"/etc/my.cnf"
	)
	
	for config in "${possible_configs[@]}"; do
		if [[ -f "$config" ]]; then
			config_file="$config"
			break
		fi
	done
	
	if [[ -z "$config_file" ]]; then
		log_error "Could not find MariaDB config file"
		log_info "Try manually: sudo find /etc -name '*my.cnf' -o -name '*mariadb*.cnf'"
		return 1
	fi
	
	log_info "Found config file: $config_file"
	
	local backup_file="${config_file}.backup.$(date +%Y%m%d_%H%M%S)"
	sudo cp "$config_file" "$backup_file"
	log_success "Backed up to: $backup_file"
	
	local total_ram_gb=$(free -g | awk '/^Mem:/{print $2}')
	local suggested_buffer_gb=$((total_ram_gb * 50 / 100))
	
	log_info "Suggested buffer pool: ${suggested_buffer_gb}GB (50% of ${total_ram_gb}GB RAM)"
	
	echo -e "${YELLOW}Choose buffer pool size:${NC}"
	echo "1. Conservative: 4GB (25% of RAM)"
	echo "2. Moderate: 8GB (50% of RAM)" 
	echo "3. Aggressive: 10GB (62% of RAM)"
	echo "4. Custom size"
	echo "5. Cancel"
	
	read -p "Choice [1-5]: " choice
	
	local buffer_size=""
	case $choice in
		1) buffer_size="4G" ;;
		2) buffer_size="8G" ;;
		3) buffer_size="10G" ;;
		4) 
			read -p "Enter buffer pool size (e.g., 6G): " buffer_size
			;;
		5|*)
			log_info "Cancelled"
			return 0
			;;
	esac
	
	log_info "Adding buffer pool setting: $buffer_size"
	
	sudo sed -i '/^innodb_buffer_pool_size/d' "$config_file"
	
	if grep -q "^\[mysqld\]" "$config_file"; then
		sudo sed -i '/^\[mysqld\]/a innodb_buffer_pool_size = '"$buffer_size" "$config_file"
	else
		echo -e "\n[mysqld]\ninnodb_buffer_pool_size = $buffer_size" | sudo tee -a "$config_file" >/dev/null
	fi
	
	log_success "Configuration updated!"
	
	echo -e "${YELLOW}Next steps:${NC}"
	echo "1. sudo systemctl restart mariadb"
	echo "2. Verify: mariadb -u test_user -ptest_password -e \"SELECT @@innodb_buffer_pool_size/1024/1024/1024 AS buffer_pool_GB;\""
	echo "3. Test your import: ./rockdbutil.sh -i your_backup.tar.gz"
	echo
	echo -e "${CYAN}If you want to revert:${NC}"
	echo "sudo cp $backup_file $config_file && sudo systemctl restart mariadb"
}

verify_buffer_pool_change() {
	local mysql_cmd=$(get_mysql_command)
	
	log_step "Verifying buffer pool changes.."
	
	echo -e "${WHITE}Current MariaDB buffer pool:${NC}"
	"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -e "
	SELECT 
		'Buffer Pool Size' as Setting,
		CONCAT(ROUND(@@innodb_buffer_pool_size/1024/1024/1024, 2), ' GB') as Value,
		CASE 
			WHEN @@innodb_buffer_pool_size >= 4294967296 THEN 'Good'
			WHEN @@innodb_buffer_pool_size >= 1073741824 THEN 'Moderate' 
			ELSE 'Too small'
		END as Status;" 2>/dev/null
	
	echo -e "${WHITE}System memory usage:${NC}"
	free -h
	
	echo -e "${WHITE}MariaDB process memory:${NC}"
	ps aux | grep [m]ariadb | head -3
	
	local current_buffer_gb=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -e "SELECT ROUND(@@innodb_buffer_pool_size/1024/1024/1024, 2);" 2>/dev/null | tail -n +2)
	
	if (( $(echo "$current_buffer_gb > 1.0" | bc -l 2>/dev/null || echo "0") )); then
		log_success "Buffer pool is now ${current_buffer_gb}GB"
	else
		log_warning "Buffer pool is still small (${current_buffer_gb}GB) - may need manual config"
	fi
}

test_import_with_buffer_optimization() {
	local archive_file="$1"
	
	if [[ -z "$archive_file" ]]; then
		log_error "Archive file not specified"
		return 1
	fi
	
	log_step "Testing import performance with optimized buffer pool..."
	
	verify_buffer_pool_change
	
	local mysql_cmd=$(get_mysql_command)
	log_info "Applying basic import optimizations..."
	
	"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" << 'EOF'
SET SESSION foreign_key_checks = 0;
SET SESSION unique_checks = 0;
SET SESSION autocommit = 0;
EOF
	
	local start_time=$(date +%s)
	log_info "Starting timed import test at $(date)"
	
	import_database "$archive_file" "true"
	
	local end_time=$(date +%s)
	local duration=$((end_time - start_time))
	local minutes=$((duration / 60))
	local seconds=$((duration % 60))
	
	# Restore safe settings
	"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" << 'EOF'
SET SESSION foreign_key_checks = 1;
SET SESSION unique_checks = 1;
SET SESSION autocommit = 1;
EOF
	
	log_success "Import completed in ${minutes}m ${seconds}s"
}

retry_failed_imports() {
	local mysql_cmd="$1"

	if [[ ! -s "$ERROR_REPORT" ]]; then
		log_success "No failed imports to retry"
		return 0
	fi

	log_step "Retrying failed imports sequentially (no parallelism = no lock conflicts).."

	local failed_files=()
	local seen_files=()
	while IFS= read -r line; do
		if [[ "$line" =~ ^FAILED:\ ([^[:space:]]+)\.sql ]]; then
			local table_name="${BASH_REMATCH[1]}"
			local sql_file="$EXTRACT_DIR/${table_name}.sql"
			if [[ -f "$sql_file" ]] && ! printf '%s\n' "${seen_files[@]}" | grep -qxF "$sql_file"; then
				failed_files+=("$sql_file")
				seen_files+=("$sql_file")
			fi
		fi
	done < "$ERROR_REPORT"

	if [[ ${#failed_files[@]} -eq 0 ]]; then
		log_warning "No valid failed files found to retry"
		return 0
	fi

	log_info "Retrying ${#failed_files[@]} failed tables sequentially.."

	# Clear the error report so import_single_file can log fresh failures
	> "$ERROR_REPORT"

	local retry_success=0
	local retry_failed=0

	for sql_file in "${failed_files[@]}"; do
		local table_name=$(basename "$sql_file" .sql)
		local file_size_mb=$(( $(stat -c%s "$sql_file" 2>/dev/null || stat -f%z "$sql_file" 2>/dev/null || echo 0) / 1024 / 1024 ))

		log_progress "Retrying: $table_name (${file_size_mb}MB)"

		local start_time=$(date +%s)
		# Route through import_single_file so large tables still get chunked import
		if import_single_file "$sql_file" "$mysql_cmd"; then
			local end_time=$(date +%s)
			local duration=$((end_time - start_time))
			log_success "$table_name completed (${duration}s)"
			(( retry_success++ )) || true
		else
			log_error "Still failed: $table_name"
			(( retry_failed++ )) || true
		fi
	done

	if [[ $retry_failed -eq 0 ]]; then
		log_success "All failed imports successfully retried! ($retry_success/$retry_success)"
	elif [[ $retry_success -gt 0 ]]; then
		log_warning "Partial success: $retry_success succeeded, $retry_failed still failed"
		return 1
	else
		log_error "All retry attempts failed ($retry_failed/$retry_failed)"
		return 1
	fi

	return 0
}

# === FK DEPENDENCY RESOLVER ===
# Recursively expands a table list to include all Foreign Key (FK) parent tables, up to a depth cap.
# Prints the expanded list (original + parents) to stdout, one table per line, deduplicated.
#
# Args:
#   $1  - comma-quoted IN list of table names (e.g. 'tbl_a','tbl_b')
#   $2  - current recursion depth (caller passes 0)
#
# Writes to caller-scoped arrays FK_ADDED_TABLES and FK_WARNED_MISSING via temp files
# rather than subshell vars, because this function is called from selective_restore()
# which needs to accumulate results across recursive calls.
resolve_fk_dependencies() {
	local in_list="$1"
	local depth="${2:-0}"
	local max_depth=10

	if [[ "$depth" -ge "$max_depth" ]]; then
		log_warning "FK resolution reached depth cap ($max_depth levels) - stopping recursion"
		return 0
	fi

	local mysql_cmd
	mysql_cmd=$(get_mysql_command)

	local parents
	parents=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -sN \
		-e "SELECT DISTINCT REFERENCED_TABLE_NAME
		    FROM information_schema.KEY_COLUMN_USAGE
		    WHERE TABLE_SCHEMA = '${DB_NAME}'
		    AND TABLE_NAME IN (${in_list})
		    AND REFERENCED_TABLE_NAME IS NOT NULL;" 2>/dev/null)

	[[ -z "$parents" ]] && return 0

	local new_parents=()
	local next_in_list_parts=()

	while IFS= read -r parent; do
		[[ -z "$parent" ]] && continue

		# Skip if already in the known set (tracked in temp file to survive subshells)
		if grep -qxF "$parent" "$BASE_DIR/.fk_seen_tables" 2>/dev/null; then
			continue
		fi

		echo "$parent" >> "$BASE_DIR/.fk_seen_tables"
		new_parents+=("$parent")
		next_in_list_parts+=("'${parent}'")

		if [[ -f "$EXTRACT_DIR/${parent}.sql" ]]; then
			echo "$parent" >> "$BASE_DIR/.fk_added_tables"
			log_info "FK dependency added: ${parent} (depth $((depth + 1)))"
		else
			echo "$parent" >> "$BASE_DIR/.fk_missing_tables"
			log_warning "FK parent table '${parent}' not found in archive - cannot restore"
		fi
	done <<< "$parents"

	if [[ ${#next_in_list_parts[@]} -gt 0 ]]; then
		local next_in_list
		next_in_list=$(printf '%s,' "${next_in_list_parts[@]}")
		next_in_list="${next_in_list%,}"
		resolve_fk_dependencies "$next_in_list" $((depth + 1))
	fi
}

# === SELECTIVE RESTORE ===
# Restores only a subset of tables from an archive, determined either by binlog scanning
# (auto mode) or a caller-supplied comma-separated table list (manual mode).
#
# After extraction, non-matching .sql files are moved to EXTRACT_DIR/__held/ (never deleted).
# FK parent tables are resolved recursively and added to the restore set automatically.
# The existing import pipeline runs unchanged on whatever remains in EXTRACT_DIR.
#
# Args:
#   $1  - archive file path (.tar.gz)
#   $2  - auto_cleanup (true/false)
#   $3  - manual tables string (comma-separated, empty string for auto mode)
selective_restore() {
	local archive_file="$1"
	local auto_cleanup="$2"
	local manual_tables="$3"

	local restore_start_time
	restore_start_time=$(date +%s)

	if [[ -z "$archive_file" ]]; then
		log_error "--selective-restore always requires -i <archive>"
		show_usage
		exit 1
	fi

	if [[ ! -f "$archive_file" ]]; then
		log_error "Archive file not found: $archive_file"
		exit 1
	fi

	local binlogparser_path
	binlogparser_path="$(dirname "$(realpath "$0")")/binlogparser.sh"

	log_step "Starting selective restore.."

	# -- Extract archive (reuse existing logic via import_database prerequisites) --
	check_command "parallel"
	check_command "bc"
	test_db_connection

	# Clean any stale optimization state files from previous interrupted runs.
	# If left behind they cause restore_import_optimizations to write back wrong values.
	rm -f "$BASE_DIR"/.original_*

	setup_directories
	if [[ -d "$EXTRACT_DIR" ]] && [[ "$(ls -A "$EXTRACT_DIR" 2>/dev/null)" ]]; then
		log_warning "Extract directory contains files. Removing old files.."
		rm -rf "$EXTRACT_DIR"/*
	fi
	rm -rf "$ERROR_LOG_DIR"/*

	IMPORT_TYPE=""
	resolve_import_file "$archive_file"

	if [[ "$IMPORT_TYPE" == "split_sql" ]]; then
		log_step "Detected monolithic SQL dump (.sql.gz) - invoking sqlsplit for selective restore.."

		if [[ ! -f "$SQLSPLIT_PATH" ]]; then
			log_error "sqlsplit.sh not found at: $SQLSPLIT_PATH"
			log_info "Place sqlsplit.sh in the same directory as rockdbutil.sh"
			exit 1
		fi

		if [[ ! -x "$SQLSPLIT_PATH" ]]; then
			log_error "sqlsplit.sh is not executable: $SQLSPLIT_PATH"
			log_info "Run: chmod +x $SQLSPLIT_PATH"
			exit 1
		fi

		local sqlsplit_csv_flag=""
		[[ "$EXPORT_FORMAT" == "csv" ]] && sqlsplit_csv_flag="--csv"
		bash "$SQLSPLIT_PATH" "$archive_file" --direct-to-dir "$EXTRACT_DIR" --db-name "$DB_NAME" --threshold "$LARGE_TABLE_THRESHOLD_MB" $sqlsplit_csv_flag
		IMPORT_TYPE="split"
	fi

	if [[ "$IMPORT_TYPE" == "archive" ]]; then
		log_step "Extracting archive: $archive_file"
		if tar -xzf "$archive_file" -C "$EXTRACT_DIR" 2>/dev/null; then
			local extracted_count
			extracted_count=$(find "$EXTRACT_DIR" -maxdepth 1 -name "*.sql" ! -name "__*.sql" | wc -l)
			log_success "Extracted $extracted_count table SQL files"
		else
			log_error "Failed to extract archive"
			exit 1
		fi
	else
		local extracted_count
		extracted_count=$(find "$EXTRACT_DIR" -maxdepth 1 -name "*.sql" ! -name "__*.sql" | wc -l)
		log_success "Split complete - $extracted_count SQL files ready"
	fi

	if [[ -f "$EXTRACT_DIR/__export_meta.txt" ]]; then
		cp "$EXTRACT_DIR/__export_meta.txt" "$BASE_DIR/last_export_meta.txt"
		log_info "Export metadata saved to: $BASE_DIR/last_export_meta.txt"
	fi

	# -- Build restore table list --
	local restore_tables=()

	if [[ -n "$manual_tables" ]]; then
		log_step "Manual mode - using supplied table list"
		IFS=',' read -ra restore_tables <<< "$manual_tables"
		restore_tables=("${restore_tables[@]// /}")
	else
		log_step "Auto mode - scanning binlog for changed tables.."

		local meta_file="$BASE_DIR/last_export_meta.txt"
		if [[ ! -f "$meta_file" ]]; then
			log_error "No export metadata found at: $meta_file"
			log_info "Run a full export first, or supply --tables for manual mode"
			exit 1
		fi

		if [[ ! -f "$binlogparser_path" ]]; then
			log_error "binlogparser.sh not found at: $binlogparser_path"
			log_info "Place binlogparser.sh in the same directory as rockdbutil.sh"
			exit 1
		fi

		if [[ ! -x "$binlogparser_path" ]]; then
			log_error "binlogparser.sh is not executable: $binlogparser_path"
			log_info "Run: chmod +x $binlogparser_path"
			exit 1
		fi

		local binlog_output
		binlog_output=$(bash "$binlogparser_path" \
			--from-export "$meta_file" \
			--db-name "$DB_NAME" \
			-db "$CURRENT_DB_PROFILE" 2>/dev/null \
			| grep -v '^$' || true)

		if [[ -z "$binlog_output" ]]; then
			log_warning "Binlog scan found no changed tables since last export"
			log_info "Nothing to restore - exiting"
			exit 0
		fi

		while IFS= read -r entry; do
			[[ -z "$entry" ]] && continue
			local tbl="${entry##*.}"
			restore_tables+=("$tbl")
		done <<< "$binlog_output"

		log_success "Binlog scan found ${#restore_tables[@]} changed table(s)"
	fi

	if [[ ${#restore_tables[@]} -eq 0 ]]; then
		log_error "Restore table list is empty"
		exit 1
	fi

	# -- FK dependency resolution --
	log_step "Resolving FK dependencies.."

	rm -f "$BASE_DIR/.fk_seen_tables" "$BASE_DIR/.fk_added_tables" "$BASE_DIR/.fk_missing_tables"

	# Seed seen file with the initial restore set so parents-of-initial-tables are still found
	# but the initial tables themselves are not double-added
	printf '%s\n' "${restore_tables[@]}" > "$BASE_DIR/.fk_seen_tables"

	local in_list_parts=()
	local tbl
	for tbl in "${restore_tables[@]}"; do
		in_list_parts+=("'${tbl}'")
	done
	local in_list
	in_list=$(printf '%s,' "${in_list_parts[@]}")
	in_list="${in_list%,}"

	resolve_fk_dependencies "$in_list" 0

	local fk_added=()
	if [[ -f "$BASE_DIR/.fk_added_tables" ]]; then
		while IFS= read -r t; do
			[[ -n "$t" ]] && fk_added+=("$t")
		done < "$BASE_DIR/.fk_added_tables"
	fi

	local final_restore_tables=("${restore_tables[@]}" "${fk_added[@]}")

	rm -f "$BASE_DIR/.fk_seen_tables" "$BASE_DIR/.fk_added_tables"

	# -- Estimate full import size for summary comparison --
	local total_archive_bytes=0
	local restore_archive_bytes=0
	local restore_set_lookup
	restore_set_lookup=$(printf '\n%s' "${final_restore_tables[@]}")

	# Size estimation uses .csv files for CSV archives, .sql files for SQL archives
	local size_ext=".sql"
	[[ "$archive_export_format" == "csv" ]] && size_ext=".csv"

	while IFS= read -r data_file; do
		local file_bytes
		file_bytes=$(stat -c%s "$data_file" 2>/dev/null || echo 0)
		total_archive_bytes=$((total_archive_bytes + file_bytes))
		local tname
		tname=$(basename "$data_file" "$size_ext")
		if echo "$restore_set_lookup" | grep -qxF "$tname"; then
			restore_archive_bytes=$((restore_archive_bytes + file_bytes))
		fi
	done < <(find "$EXTRACT_DIR" -maxdepth 1 -name "*${size_ext}" ! -name "__*.sql")

	# -- Move non-restore tables to __held/ --
	local held_dir="$EXTRACT_DIR/__held"
	mkdir -p "$held_dir"

	local skipped_tables=()

	if [[ "$archive_export_format" == "csv" ]]; then
		# CSV archive: hold .csv files and matching .schema.sql files for non-restore tables
		while IFS= read -r csv_file; do
			local tname
			tname=$(basename "$csv_file" .csv)
			if ! echo "$restore_set_lookup" | grep -qxF "$tname"; then
				mv "$csv_file" "$held_dir/"
				[[ -f "$EXTRACT_DIR/${tname}.schema.sql" ]] && mv "$EXTRACT_DIR/${tname}.schema.sql" "$held_dir/"
				skipped_tables+=("$tname")
			fi
		done < <(find "$EXTRACT_DIR" -maxdepth 1 -name "*.csv")

		# Also hold SQL fallback files (blob tables) that are not in restore set
		while IFS= read -r sql_file; do
			local tname
			tname=$(basename "$sql_file" .sql)
			if ! echo "$restore_set_lookup" | grep -qxF "$tname"; then
				mv "$sql_file" "$held_dir/" 2>/dev/null || true
			fi
		done < <(find "$EXTRACT_DIR" -maxdepth 1 -name "*.sql" ! -name "*.schema.sql" ! -name "__*.sql")
	else
		while IFS= read -r sql_file; do
			local tname
			tname=$(basename "$sql_file" .sql)
			if ! echo "$restore_set_lookup" | grep -qxF "$tname"; then
				mv "$sql_file" "$held_dir/"
				skipped_tables+=("$tname")
			fi
		done < <(find "$EXTRACT_DIR" -maxdepth 1 -name "*.sql" ! -name "__*.sql")
	fi

	local restore_count=${#final_restore_tables[@]}
	local skipped_count=${#skipped_tables[@]}

	log_success "Restore set: $restore_count table(s) | Held: $skipped_count table(s)"

	# Regenerate the large table manifest for the restore set.
	# The manifest is written by sqlsplit during .sql.gz splits but is never packed into
	# .tar.gz archives - selective restore must rebuild it from file sizes so that
	# import_single_file correctly defers large tables to chunk_import_file.
	#
	# For CSV archives, scan .csv file sizes instead of .sql file sizes.
	local archive_export_format="sql"
	local archive_csv_load_mode="server"
	if [[ -f "$EXTRACT_DIR/__export_meta.txt" ]]; then
		local meta_format
		meta_format=$(grep "^export_format=" "$EXTRACT_DIR/__export_meta.txt" | cut -d'=' -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' || true)
		[[ "$meta_format" == "csv" ]] && archive_export_format="csv"

		local meta_load_mode
		meta_load_mode=$(grep "^csv_load_mode=" "$EXTRACT_DIR/__export_meta.txt" | cut -d'=' -f2 | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' || true)
		[[ -n "$meta_load_mode" ]] && archive_csv_load_mode="$meta_load_mode"
	fi

	local manifest_file="$EXTRACT_DIR/__large_tables.txt"
	rm -f "$manifest_file"

	if [[ "$archive_export_format" == "csv" ]]; then
		while IFS= read -r csv_file; do
			local file_bytes
			file_bytes=$(stat -c%s "$csv_file" 2>/dev/null || echo 0)
			local file_mb=$(( file_bytes / 1048576 ))
			if [[ $file_mb -ge $LARGE_TABLE_THRESHOLD_MB ]]; then
				basename "$csv_file" .csv >> "$manifest_file"
			fi
		done < <(find "$EXTRACT_DIR" -maxdepth 1 -name "*.csv")
	else
		while IFS= read -r sql_file; do
			local file_bytes
			file_bytes=$(stat -c%s "$sql_file" 2>/dev/null || echo 0)
			local file_mb=$(( file_bytes / 1048576 ))
			if [[ $file_mb -ge $LARGE_TABLE_THRESHOLD_MB ]]; then
				basename "$sql_file" .sql >> "$manifest_file"
			fi
		done < <(find "$EXTRACT_DIR" -maxdepth 1 -name "*.sql" ! -name "__*.sql")
	fi

	if [[ -f "$manifest_file" ]]; then
		local large_count
		large_count=$(wc -l < "$manifest_file")
		log_info "Large tables in restore set flagged for chunked import: $large_count (>${LARGE_TABLE_THRESHOLD_MB}MB)"
	fi

	# -- Run import pipeline --
	log_step "Running import pipeline on selective restore set.."

	local buffer_info
	buffer_info=$(get_optimal_buffer_size)
	local current_buffer_gb
	current_buffer_gb=$(echo "$buffer_info" | cut -d: -f1)
	local suggested_buffer_gb
	suggested_buffer_gb=$(echo "$buffer_info" | cut -d: -f2)

	local use_buffer_optimization=false
	local cleanup_buffer=false
	trap 'if [[ "${cleanup_buffer:-false}" == "true" ]]; then restore_import_optimizations; fi' EXIT

	if [[ $(echo "$suggested_buffer_gb > $current_buffer_gb" | bc 2>/dev/null) == "1" ]]; then
		if [[ "$auto_cleanup" == "true" ]]; then
			if apply_import_optimizations "$suggested_buffer_gb"; then
				use_buffer_optimization=true
				cleanup_buffer=true
			fi
		else
			read -p "$(echo -e "${YELLOW}Temporarily increase buffer pool to ${suggested_buffer_gb}GB? [Y/n]:${NC} ")" -n 1 -r
			echo
			if [[ ! $REPLY =~ ^[Nn]$ ]]; then
				if apply_import_optimizations "$suggested_buffer_gb"; then
					use_buffer_optimization=true
					cleanup_buffer=true
				fi
			fi
		fi
	fi

	local mysql_cmd
	mysql_cmd=$(get_mysql_command)
	local threads
	threads=$(get_thread_count)
	log_info "Using $threads parallel threads"

	> "$ERROR_REPORT"
	> "$SUCCESS_LOG"

	register_worker_exports

	if [[ "$archive_export_format" == "csv" ]]; then
		CSV_LOAD_MODE="$archive_csv_load_mode"
		export CSV_LOAD_MODE
		log_info "CSV archive detected (load_mode: $CSV_LOAD_MODE) - using three-phase selective restore"
		_import_database_csv "$mysql_cmd" "$threads" "false" "$use_buffer_optimization"
	else
		_import_database_sql "$mysql_cmd" "$threads" "false" "$use_buffer_optimization"
	fi

	rm -f "$BASE_DIR/.import_success"

	# -- Restore or discard held files --
	if [[ "$auto_cleanup" == "true" ]]; then
		rm -rf "$held_dir"
		rm -rf "$EXTRACT_DIR"/*
		rm -f "$ERROR_REPORT" "$SUCCESS_LOG"
		rm -rf "$ERROR_LOG_DIR"/*
		log_success "Held files discarded (--auto-cleanup)"
	else
		find "$held_dir" -maxdepth 1 \( -name "*.sql" -o -name "*.csv" \) -exec mv {} "$EXTRACT_DIR/" \;
		rmdir "$held_dir" 2>/dev/null || true
		log_info "Held files restored to: $EXTRACT_DIR"
	fi

	# -- Advance the binlog baseline to now so the next selective restore only sees new changes --
	local mysql_cmd_pos
	mysql_cmd_pos=$(get_mysql_command)
	local new_master_status
	new_master_status=$("$mysql_cmd_pos" -u "$DB_USER" -p"$DB_PASS" -sN \
		-e "SHOW MASTER STATUS;" 2>/dev/null | head -1)

	if [[ -n "$new_master_status" ]]; then
		local new_binlog_file new_binlog_position
		new_binlog_file=$(echo "$new_master_status" | awk '{print $1}')
		new_binlog_position=$(echo "$new_master_status" | awk '{print $2}')
		cat > "$BASE_DIR/last_export_meta.txt" <<EOF
binlog_file=${new_binlog_file}
binlog_position=${new_binlog_position}
export_timestamp=$(date +"%Y-%m-%d %H:%M:%S")
db_name=${DB_NAME}
rockdbutil_version=1.0
export_format=${archive_export_format}
csv_load_mode=${archive_csv_load_mode}
EOF
		log_info "Binlog baseline advanced to: ${new_binlog_file}:${new_binlog_position}"
	else
		log_warning "Could not advance binlog baseline - next selective restore may re-restore these tables"
	fi

	rm -f "$BASE_DIR/.fk_missing_tables"

	# -- Summary output --
	local restore_end_time
	restore_end_time=$(date +%s)
	local duration=$((restore_end_time - restore_start_time))
	local minutes=$((duration / 60))
	local seconds=$((duration % 60))

	local total_mb=$(( (total_archive_bytes + 524288) / 1048576 ))
	local restore_mb=$(( (restore_archive_bytes + 524288) / 1048576 ))
	local skipped_mb=$(( total_mb - restore_mb ))

	echo
	echo -e "${GREEN}╔══════════════════════════════════════════════════════╗${NC}"
	echo -e "${GREEN}║           Selective restore completed                ║${NC}"
	echo -e "${GREEN}╚══════════════════════════════════════════════════════╝${NC}"
	echo -e "  Database        : ${WHITE}$DB_NAME${NC}"
	echo -e "  Restored        : ${WHITE}$restore_count table(s) (${restore_mb}MB)${NC}"
	if [[ ${#fk_added[@]} -gt 0 ]]; then
		echo -e "  FK deps added   : ${WHITE}${#fk_added[@]} table(s): $(printf '%s ' "${fk_added[@]}")${NC}"
	fi
	echo -e "  Skipped         : ${WHITE}$skipped_count table(s) (${skipped_mb}MB held)${NC}"
	echo -e "  Duration        : ${WHITE}${minutes}m ${seconds}s${NC}"
	if [[ -f "$BASE_DIR/.fk_missing_tables" ]]; then
		echo -e "  ${YELLOW}FK parents missing from archive:${NC}"
		while IFS= read -r missing; do
			echo -e "    ${YELLOW}• $missing${NC}"
		done < "$BASE_DIR/.fk_missing_tables"
	fi
	echo
}

# === USAGE FUNCTION ===
show_usage() {
	echo -e "${WHITE}rockdbutil - MariaDB/MySQL Import/Export Tool${NC}"
	echo -e "${WHITE}Usage:${NC}"
	echo "  $0 --setup                              # Initial setup (creates config file and directories)"
	echo "  $0 -e [-db profile] [-d] [--sql]        # Export database"
	echo "  $0 -i <archive> [-db profile] [-d]      # Import database"
	echo "                                          # Accepts .tar.gz (rockdbutil export)"
	echo "                                          # or .sql.gz (monolithic mysqldump/mariadb-dump)"
	echo "  $0 --list-profiles                      # List available database profiles"
	echo "  $0 --test-connection [-db profile]      # Test database connection"
	echo
	echo -e "${WHITE}Options:${NC}"
	echo "  -db, --database PROFILE                 # Use specific database profile"
	echo "                                          # If not specified, uses 'default' profile"
	echo "  -d, --auto-cleanup                      # Automatic cleanup of temporary files"
	echo "                                          # Also applies buffer pool optimization for imports"
	echo "  -e, --export                            # Export database to compressed archive"
	echo "                                          # Default format: CSV (faster at scale)"
	echo "  --sql                                   # Force SQL export for this run (overrides config)"
	echo "                                          # Use when CSV is not available or for legacy compat"
	echo "  -i, --import FILE                       # Import database from compressed archive"
	echo "                                          # Format auto-detected from archive metadata"
	echo "  -h, --help                              # Show this help message"
	echo
	echo -e "${WHITE}Export formats:${NC}"
	echo "  CSV (default)  - Schema as .schema.sql + data as .csv via SELECT INTO OUTFILE"
	echo "                   5-10x faster than SQL at 70GB+ scale via LOAD DATA INFILE on import"
	echo "                   Tables with blob/binary columns fall back to SQL automatically"
	echo "  SQL (--sql)    - Full per-table .sql files via mysqldump (legacy behaviour)"
	echo "                   Use if secure_file_priv=NULL and local_infile=OFF on the server"
	echo
	echo -e "${WHITE}Examples:${NC}"
	echo "  $0 --setup                              # First time setup"
	echo "  $0 -e                                   # Export as CSV (default - fastest)"
	echo "  $0 -e --sql                             # Export as SQL (legacy format)"
	echo "  $0 -e -db production                    # Export using 'production' database profile"
	echo "  $0 -e -d                                # Export with automatic cleanup"
	echo "  $0 -i backup.tar.gz                     # Import rockdbutil export (CSV or SQL, auto-detected)"
	echo "  $0 -i db_backup.sql.gz                  # Import monolithic prod dump to default database"
	echo "  $0 -i backup.tar.gz -db staging         # Import to 'staging' database profile"
	echo "  $0 -i backup.tar.gz -d                  # Import with auto-cleanup and optimization"
	echo "  $0 -d -i backup.tar.gz -db production   # Auto-optimized import to production"
	echo
	echo -e "${WHITE}Database Profiles:${NC}"
	echo "  Multiple database configurations can be stored in the config file"
	echo "  Profile format: profilename_db_name, profilename_db_user, profilename_db_pass"
	echo "  Examples: production_db_name, staging_db_name, dev_db_name"
	echo
	echo -e "${WHITE}Configuration:${NC}"
	echo "  Config file: ~/.config/rockdbutil.conf"
	echo "  Edit with: vim ~/.config/rockdbutil.conf or nano ~/.config/rockdbutil.conf"
	echo "  List profiles: $0 --list-profiles"
	echo "  Test connection: $0 --test-connection -db profilename"
	echo
	echo -e "${WHITE}Directories:${NC}"
	echo "  Base: ~/database_operations/ (configurable in config file)"
	echo "  Dumps: ~/database_operations/dumps/"
	echo "  Restore: ~/database_operations/restore/"
	echo "  CSV staging: ~/database_operations/csv_export/"
	echo "  Logs: ~/database_operations/logs/"
	echo
	echo -e "${WHITE}Performance Features:${NC}"
	echo "  • CSV export/import via SELECT INTO OUTFILE + LOAD DATA INFILE (default)"
	echo "  • Parallel processing (auto-detects CPU cores)"
	echo "  • Automatic buffer pool optimization for imports"
	echo "  • Intelligent retry logic for failed imports"
}

# === MAIN SCRIPT ===
main() {
	if [[ $EUID -eq 0 ]]; then
		log_warning "Running as root. This is not recommended for database operations"
	fi
	
	# Parse arguments
	local auto_cleanup=false
	local database_profile="default"
	local command=""
	local archive_file=""
	local selective_tables=""
	local force_sql_export=false

	while [[ $# -gt 0 ]]; do
		case $1 in
			-d|--auto-cleanup)
				auto_cleanup=true
				shift
				;;
			-db|--database)
				database_profile="$2"
				shift 2
				;;
			-e|export)
				command="export"
				shift
				;;
			-i|import)
				archive_file="$2"
				[[ "$command" != "selective-restore" ]] && command="import"
				shift 2
				;;
			--sql)
				force_sql_export=true
				shift
				;;
			--setup|setup)
				command="setup"
				shift
				;;
			--list-profiles)
				command="list-profiles"
				shift
				;;
			--test-connection)
				command="test-connection"
				shift
				;;
			--help|-h|help)
				command="help"
				shift
				;;
			--test-buffer)
				command="test-buffer"
				shift
				;;
			--verify-buffer)
				command="verify-buffer"
				shift
				;;
			--test-import-buffer)
				command="test-import-buffer"
				archive_file="$2"
				shift 2
				;;
			--selective-restore)
				command="selective-restore"
				shift
				;;
			--tables)
				selective_tables="$2"
				shift 2
				;;
			*)
				log_error "Invalid option: $1"
				show_usage
				exit 1
				;;
		esac
	done

	if [[ "$command" == "setup" ]]; then
		setup_rockdbutil
		return
	fi

	if [[ "$command" != "help" && "$command" != "list-profiles" ]]; then
		load_database_config "$database_profile"

		if [[ "$auto_cleanup" == "false" && "$AUTO_CLEANUP_CONFIG" == "true" ]]; then
			auto_cleanup=true
		fi

		# --sql flag overrides config export_format for this run only
		if [[ "$force_sql_export" == "true" ]]; then
			EXPORT_FORMAT="sql"
			log_info "SQL export mode forced via --sql flag"
		fi
	fi
	
	case "$command" in
		export)
			export_database "$auto_cleanup"
			;;
		import)
			import_database "$archive_file" "$auto_cleanup"
			;;
		config)
			configure_database
			;;
		list-profiles)
			list_database_profiles
			;;
		test-connection)
			test_db_connection
			;;
		test-buffer)
			test_larger_buffer_pool
			;;
		verify-buffer)
			verify_buffer_pool_change
			;;
		test-import-buffer)
			test_import_with_buffer_optimization "$archive_file"
			;;
		selective-restore)
			selective_restore "$archive_file" "$auto_cleanup" "$selective_tables"
			;;
		help|"")
			show_usage
			;;
		*)
			log_error "No command specified"
			show_usage
			exit 1
			;;
	esac
}

main "$@"
