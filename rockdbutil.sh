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
}

setup_directories() {
	if [[ ! -d "$BASE_DIR" ]]; then
		mkdir -p "$BASE_DIR"
		log_success "Created database operations directory: $BASE_DIR"
	fi
	
	mkdir -p "$DUMP_DIR"
	mkdir -p "$EXTRACT_DIR"
	mkdir -p "$ERROR_LOG_DIR"
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
	
	local mysql_cmd=$(get_mysql_command)
	local mysqldump_cmd=$(get_mysqldump_command)
	
	setup_directories
	if [[ -d "$DUMP_DIR" ]] && [[ "$(ls -A "$DUMP_DIR" 2>/dev/null)" ]]; then
		log_warning "Dump directory contains files. Removing old files.."
		rm -rf "$DUMP_DIR"/*
	fi
	log_success "Using dump directory: $DUMP_DIR"
	
	log_info "Retrieving table list from database: $DB_NAME"
	local tables
	if ! tables=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -e "SHOW TABLES IN \`$DB_NAME\`;" 2>/dev/null | tail -n +2); then
		log_error "Failed to retrieve table list from database: $DB_NAME"
		exit 1
	fi
	
	if [[ -z "$tables" ]]; then
		log_error "No tables found in database: $DB_NAME"
		exit 1
	fi
	
	local table_count=$(echo "$tables" | wc -l)
	log_success "Found $table_count tables to export"
	local exported=0
	
	log_step "Exporting tables..."
	while IFS= read -r table; do
		[[ -z "$table" ]] && continue
		log_progress "Dumping table: $table"
		if "$mysqldump_cmd" -u "$DB_USER" -p"$DB_PASS" \
			--single-transaction --skip-lock-tables \
			"$DB_NAME" "$table" > "$DUMP_DIR/$table.sql" 2>/dev/null; then
			exported=$((exported + 1))
		else
			log_warning "Failed to dump table: $table"
		fi
	done <<< "$tables"

	log_success "Successfully exported $exported/$table_count tables"

	log_step "Exporting triggers and routines..."
	if "$mysqldump_cmd" -u "$DB_USER" -p"$DB_PASS" \
		--single-transaction --skip-lock-tables \
		--no-data --no-create-info \
		--routines --triggers \
		"$DB_NAME" > "$DUMP_DIR/__routines_and_triggers.sql" 2>/dev/null; then
		local routine_count
		routine_count=$(grep -c "^CREATE.*PROCEDURE\|^CREATE.*FUNCTION\|^CREATE.*TRIGGER" "$DUMP_DIR/__routines_and_triggers.sql" 2>/dev/null || true)
		if [[ "$routine_count" -gt 0 ]]; then
			log_success "Exported ${routine_count} routine/trigger definition(s)"
		else
			rm -f "$DUMP_DIR/__routines_and_triggers.sql"
			log_info "No triggers or routines found in $DB_NAME"
		fi
	else
		log_warning "Failed to export triggers and routines"
	fi
	
	log_step "Creating compressed archive..."
	if tar -czf "$ARCHIVE_NAME" -C "$DUMP_DIR" . 2>/dev/null; then
		local archive_size=$(du -h "$ARCHIVE_NAME" | cut -f1)
		log_success "Archive created: $ARCHIVE_NAME ($archive_size)"
	else
		log_error "Failed to create archive"
		exit 1
	fi
	
	# Cleanup option
if [[ "$auto_cleanup" == "true" ]]; then
		rm -rf "$DUMP_DIR"/*
		rm -f "$ERROR_REPORT" "$SUCCESS_LOG"
		rm -rf "$ERROR_LOG_DIR"/*
		log_success "Temporary files automatically cleaned up"
	fi
}

# === IMPORT FILE RESOLVER ===
# Detects whether the supplied file is a .sql.gz (monolithic prod dump) or a
# .tar.gz (rockdbutil export). For .sql.gz files, sqlsplit.sh is called to split
# the dump directly into EXTRACT_DIR, bypassing the tar extraction step entirely.
#
# Sets the caller-scoped variable IMPORT_TYPE to "split" or "archive".
# Must NOT be called inside $() - sqlsplit needs to run in the foreground
# so its output is visible and it has access to the real filesystem.
resolve_import_file() {
	local archive_file="$1"

	if [[ "$archive_file" == *.sql.gz ]]; then
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

	local buffer_info=$(get_optimal_buffer_size)
	local current_buffer_gb=$(echo "$buffer_info" | cut -d: -f1)
	local suggested_buffer_gb=$(echo "$buffer_info" | cut -d: -f2) 
	local total_ram_gb=$(echo "$buffer_info" | cut -d: -f3)

	local mysql_cmd=$(get_mysql_command)
	
	echo -e "${WHITE}System Memory Status:${NC}"
	echo "Total RAM: ${total_ram_gb}GB"
	echo "Available: $(free -h | awk '/^Mem:/{print $7}')"
	echo "Current Buffer Pool: ${current_buffer_gb}GB"
	
	local use_buffer_optimization=false
	local available_gb=$(free -g | awk '/^Mem:/{print $7}')
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
			# Interactive mode
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
	
	local mysql_cmd=$(get_mysql_command)
	local threads=$(get_thread_count)
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

	if [[ "$IMPORT_TYPE" == "archive" ]]; then
		log_step "Extracting archive: $archive_file"
		if tar -xzf "$archive_file" -C "$EXTRACT_DIR" 2>/dev/null; then
			local file_count=$(find "$EXTRACT_DIR" -name "*.sql" | wc -l)
			log_success "Extracted $file_count SQL files"
		else
			log_error "Failed to extract archive"
			exit 1
		fi
	else
		local file_count=$(find "$EXTRACT_DIR" -name "*.sql" | wc -l)
		log_success "Split complete - $file_count SQL files ready in extract directory"
	fi
	local sql_files=("$EXTRACT_DIR"/*.sql)
	if [[ ! -e "${sql_files[0]}" ]]; then
		log_error "No SQL files found in archive"
		exit 1
	fi
	
	log_step "Importing SQL files into database: $DB_NAME"
	log_info "This may take some time depending on database size.."

	> "$ERROR_REPORT"
	> "$SUCCESS_LOG"

	# Chunked parallel import for large table files.
	# Streams the .sql file through awk which splits INSERT rows into N equal chunks,
	# each chunk piped directly into mariadb - no intermediate chunk files written to disk.
	# The CREATE TABLE / preamble block is imported once first, then row chunks in parallel.
	chunk_import_file() {
		local sql_file="$1"
		local mysql_cmd="$2"
		local chunks="$3"
		local filename=$(basename "$sql_file"):%s/ —/-/g
		local error_log="$ERROR_LOG_DIR/${filename}.chunk.error"
		local table_name="${filename%.sql}"

		log_progress "Chunked import: $table_name (${chunks} parallel streams)"

		# Step 1: Import the schema block (everything before the first INSERT statement).
		# Stop at the INSERT INTO line itself - the dump format has INSERT INTO `tbl` VALUES
		# on its own line followed by data rows starting with (, so it must not include it.
		awk '/^LOCK TABLES / { exit } { print }' "$sql_file" \
			| "$mysql_cmd" --batch --silent -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" 2>"$error_log"

		if [[ $? -ne 0 ]]; then
			echo "FAILED: $filename (schema phase)" >> "${ERROR_REPORT}.${table_name}"
			[[ -s "$error_log" ]] && sed 's/^/    /' "$error_log" >> "${ERROR_REPORT}.${table_name}"
			return 1
		fi

		# Step 2: Extract just the INSERT header line (e.g. INSERT INTO `tbl` VALUES)
		# and count total data rows so we can divide evenly across chunks.
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

			# Cleanup per-chunk error logs
			local i=0
			while [[ $i -lt ${#chunk_errors[@]} ]]; do
				rm -f "${chunk_errors[$i]}"
				(( i++ )) || true
			done

			# If chunks failed due to lock contention, truncate and retry sequentially.
			# Parallel inserts on tables with unique/primary key indexes cause deadlocks -
			# sequential retry eliminates contention while preserving the transaction-size
			# benefit of chunking. Tables without contention never reach this path.
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

			# On any remaining failure, snapshot the error log for debugging
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

	import_single_file() {
		local sql_file="$1"
		local filename=$(basename "$sql_file")
		local mysql_cmd="$2"
		local error_log="$ERROR_LOG_DIR/${filename}.error"
		local max_retries=3
		local retry=0

		# Route large tables through chunked parallel import
		local manifest="$EXTRACT_DIR/__large_tables.txt"
		local table_name="${filename%.sql}"
		if [[ -f "$manifest" ]] && grep -qx "$table_name" "$manifest"; then
			# Signal that this large table should be deferred - do not import inline.
			# The main import loop handles large tables after all small tables complete.
			grep -qxF "$table_name" "$EXTRACT_DIR/__deferred_large.txt" 2>/dev/null || echo "$table_name" >> "$EXTRACT_DIR/__deferred_large.txt"
			return 0
		fi
		
		while [[ $retry -lt $max_retries ]]; do
			if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$sql_file" 2>"$error_log"; then
				echo "$filename" >> "$SUCCESS_LOG"
				rm -f "$error_log"
				if [[ $retry -gt 0 ]]; then
					echo "SUCCESS: $filename (after $retry retries)" >&2
				fi
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
		if [[ $retry -gt 0 ]]; then
			retry_text=" (after $retry retries)"
		fi
		
		echo "FAILED: $filename$retry_text" >> "$ERROR_REPORT"
		if [[ -s "$error_log" ]]; then
			echo "  Error details:" >> "$ERROR_REPORT"
			sed 's/^/    /' "$error_log" >> "$ERROR_REPORT"
		fi
		echo "" >> "$ERROR_REPORT"
		return 1
	}

	export -f import_single_file chunk_import_file log_progress log_info log_success log_warning log_error log_step
	export DB_USER DB_PASS DB_NAME ERROR_LOG_DIR ERROR_REPORT SUCCESS_LOG EXTRACT_DIR LARGE_TABLE_CHUNKS
	export INNODB_FLUSH_LOG_OPT INNODB_DOUBLEWRITE_OPT INNODB_IO_CAPACITY_OPT MAX_CONCURRENT_LARGE_TABLES
	export RED GREEN YELLOW BLUE PURPLE CYAN WHITE NC

	# Import sequences first - they are shared across tables and conflict under parallel import.
	# __sequences.sql is written by sqlsplit when a monolithic .sql.gz is the source.
	local sequences_file="$EXTRACT_DIR/__sequences.sql"
	if [[ -f "$sequences_file" && -s "$sequences_file" ]]; then
		log_step "Importing database sequences (pre-import step)..."
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$sequences_file" 2>"$ERROR_LOG_DIR/__sequences.error"; then
			log_success "Sequences imported successfully"
			rm -f "$ERROR_LOG_DIR/__sequences.error"
		else
			log_warning "Sequence import had errors (may be harmless if sequences already exist)"
			cat "$ERROR_LOG_DIR/__sequences.error" | grep -v "^$" | head -5 >&2 || true
		fi
	fi

	local total_files=$(find "$EXTRACT_DIR" -name "*.sql" ! -name "__*.sql" | wc -l)

	monitor_import_progress() {
		local total=$1
		local start_time=$(date +%s)
		local last_count=0
		local last_log_time=$start_time
		
		while true; do
			if [[ -f "$SUCCESS_LOG" ]]; then
				local completed=$(wc -l < "$SUCCESS_LOG" 2>/dev/null || echo "0")
				local current_time=$(date +%s)
				local time_since_last_log=$((current_time - last_log_time))
				
				if [[ $completed -gt $last_count && $completed -gt 0 ]]; then
					local elapsed=$((current_time - start_time))
					local minutes=$((elapsed / 60))
					local seconds=$((elapsed % 60))
					
					log_progress "Imported $completed/$total tables (${minutes}m ${seconds}s elapsed)"
					last_count=$completed
					last_log_time=$current_time
				elif [[ $time_since_last_log -ge 10 && $completed -gt 0 ]]; then
					local remaining=$((total - completed))
					log_progress "Still working: $completed/$total complete, $remaining remaining (processing large tables)"
					last_log_time=$current_time
				fi
			fi
			sleep 3
			
			# Check if import is complete
			if ! pgrep -f "parallel.*import_single_file" > /dev/null; then
				break
			fi
		done
	}

	monitor_import_progress "$total_files" &
	local monitor_pid=$!

	local import_success=false
	local sql_file_list=()
	while IFS= read -r -d '' f; do
		sql_file_list+=("$f")
	done < <(find "$EXTRACT_DIR" -maxdepth 1 -name "*.sql" ! -name "__*.sql" -print0)

	parallel -j "$threads" import_single_file {} "$mysql_cmd" ::: "${sql_file_list[@]}" || true
	kill $monitor_pid 2>/dev/null || true
	wait $monitor_pid 2>/dev/null || true

	# Import large tables sequentially after small tables complete - each gets
	# the full database to itself for its chunked parallel streams, avoiding
	# lock contention between concurrent large table imports.
	local deferred_file="$EXTRACT_DIR/__deferred_large.txt"
	if [[ -f "$deferred_file" && -s "$deferred_file" ]]; then
		# Pre-compute row counts for all large tables in parallel before import starts.
		# Eliminates the sequential awk scan inside chunk_import_file for each table.
		log_info "Pre-computing row counts for large tables..."
		while IFS= read -r table_name; do
			local sql_file="$EXTRACT_DIR/${table_name}.sql"
			awk '/^\(/ { count++ } END { print count+0 }' "$sql_file" \
				> "$EXTRACT_DIR/${table_name}.rowcount" &
		done < "$deferred_file"
		wait
		log_info "Row counts ready"

log_step "Importing large tables (${MAX_CONCURRENT_LARGE_TABLES} concurrent, ${LARGE_TABLE_CHUNKS} streams per table).."
		local large_pids=()
		local large_names=()
		local running=0
		local max_concurrent=$MAX_CONCURRENT_LARGE_TABLES

		while IFS= read -r table_name; do
			local sql_file="$EXTRACT_DIR/${table_name}.sql"
			local file_size_mb=$(( $(stat -c%s "$sql_file" 2>/dev/null || echo 0) / 1024 / 1024 ))
			log_progress "Large table: $table_name (${file_size_mb}MB)"
			chunk_import_file "$sql_file" "$mysql_cmd" "$LARGE_TABLE_CHUNKS" &
			large_pids+=($!)
			large_names+=("$table_name")
			running=$(( running + 1 ))

			if [[ $running -ge $max_concurrent ]]; then
				wait "${large_pids[0]}" || true
				large_pids=("${large_pids[@]:1}")
				large_names=("${large_names[@]:1}")
				running=$(( running - 1 ))
			fi
		done < "$deferred_file"

		# Wait for any remaining background jobs
		local p
		for p in "${large_pids[@]}"; do
			wait "$p" || true
		done
	fi

	local success_count=$(wc -l < "$SUCCESS_LOG" 2>/dev/null || echo "0")
	local total_count=$(find "$EXTRACT_DIR" -name "*.sql" ! -name "__*.sql" | wc -l)

	# Deferred large tables counted as success in SUCCESS_LOG by chunk_import_file.
	# Only check ERROR_REPORT for genuine failures - ignore the count arithmetic.
	if grep -q "^FAILED:" "$ERROR_REPORT" 2>/dev/null; then
		local failed_count=$((total_count - success_count))
		log_warning "$failed_count out of $total_count imports failed"
		log_info "Will retry failed imports sequentially.."
		if retry_failed_imports "$mysql_cmd"; then
			log_success "Failed imports successfully retried"
			import_success=true
		else
			log_error "Some imports still failed after retry attempts"
			import_success=false
		fi
	else
		log_success "All $success_count SQL files imported successfully"
		import_success=true
	fi

	if [[ "$auto_cleanup" == "true" && "$import_success" == "true" ]]; then
		rm -rf "$EXTRACT_DIR"/*
		rm -f "$ERROR_REPORT" "$SUCCESS_LOG"
		rm -rf "$ERROR_LOG_DIR"/*
		log_success "Temporary files automatically cleaned up"
	fi
	
	if [[ "$use_buffer_optimization" == "true" ]]; then
		restore_import_optimizations
	fi
	
	local import_end_time=$(date +%s)
	local import_duration=$((import_end_time - import_start_time))
	local import_minutes=$((import_duration / 60))
	local import_seconds=$((import_duration % 60))

	echo
	if [[ "$import_success" == "true" ]]; then
		echo -e "${GREEN}╔══════════════════════════════════════════╗${NC}"
		echo -e "${GREEN}║       Import completed successfully!     ║${NC}"
		echo -e "${GREEN}╚══════════════════════════════════════════╝${NC}"
		echo -e "  Database : ${WHITE}$DB_NAME${NC}"
		echo -e "  Tables   : ${WHITE}$total_files${NC}"
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
	echo -e "${GREEN}Expected improvement:${NC}"
	echo "• DB_A: 14min → 7-10min"
	echo "• DB_B: 28min → 14-20min"
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
	
	echo -e "${CYAN}Performance comparison:${NC}"
	echo "• Previous time: Your baseline (14min DB_A - 350mb, 28min DB_B - 700mb)"
	echo "• This test: ${minutes}m ${seconds}s"
	
	if [[ $duration -lt 840 ]]; then  # 14min
		echo -e "${GREEN}Significant improvement! Buffer pool optimization worked!${NC}"
	elif [[ $duration -lt 1200 ]]; then  # 20min  
		echo -e "${YELLOW}Good improvement! Some benefit from buffer pool${NC}"
	else
		echo -e "${RED}Limited improvement - may need additional optimizations${NC}"
	fi
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

# === USAGE FUNCTION ===
show_usage() {
	echo -e "${WHITE}rockdbutil - MariaDB/MySQL Import/Export Tool${NC}"
	echo -e "${WHITE}Usage:${NC}"
	echo "  $0 --setup                              # Initial setup (creates config file and directories)"
	echo "  $0 -e [-db profile] [-d]                # Export database"
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
	echo "  -i, --import FILE                       # Import database from compressed archive"
	echo "  -h, --help                              # Show this help message"
	echo
	echo -e "${WHITE}Examples:${NC}"
	echo "  $0 --setup                              # First time setup"
	echo "  $0 -e                                   # Export using default database profile"
	echo "  $0 -e -db production                    # Export using 'production' database profile"
	echo "  $0 -e -d                                # Export with automatic cleanup"
	echo "  $0 -i backup.tar.gz                     # Import rockdbutil export to default database"
	echo "  $0 -i db_backup.sql.gz                  # Import monolithic prod dump to default database"
	echo "  $0 -i backup.tar.gz -db staging         # Import to 'staging' database profile"
	echo "  $0 -i db_backup.sql.gz -db staging      # Import prod dump to 'staging' profile"
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
	echo "  Logs: ~/database_operations/logs/"
	echo
	echo -e "${WHITE}Performance Features:${NC}"
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
				command="import"
				archive_file="$2"
				shift 2
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
