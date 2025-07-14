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
DB_NAME="test_db"
DB_USER="test_user"
DB_PASS="test_password"

BASE_DIR="$HOME/database_operations"
DUMP_DIR="$BASE_DIR/dumps"
EXTRACT_DIR="$BASE_DIR/restore"
ERROR_LOG_DIR="$BASE_DIR/logs"
ERROR_REPORT="$BASE_DIR/error_report.txt"
SUCCESS_LOG="$BASE_DIR/success_log.txt"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
ARCHIVE_NAME="db_dump_${DB_NAME}_${TIMESTAMP}.tar.gz"

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
			sudo pacman -S --noconfirm "$package"
			;;
		apt)
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

test_db_connection() {
	local mysql_cmd=$(get_mysql_command)
	
	log_info "Testing database connection..."
	if ! "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -e "SELECT 1;" &> /dev/null; then
		log_error "Cannot connect to database. Please check credentials and server status."
		exit 1
	fi
	log_success "Database connection successful."
}

get_thread_count() {
	local total_threads
	if command -v nproc &> /dev/null; then
		total_threads=$(nproc)
	elif command -v lscpu &> /dev/null; then
		total_threads=$(lscpu | grep '^CPU(s):' | awk '{print $2}')
	else
		total_threads=4
	fi
	
	# Use total threads - 2, but minimum of 1
	local threads=$((total_threads - 2))
	if [[ $threads -lt 1 ]]; then
		threads=1
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

apply_temporary_buffer_optimization() {
	local target_buffer_gb="$1"

	if ! sudo -n true 2>/dev/null; then
		log_info "Requesting sudo access for buffer pool optimization.."
		sudo -v
	fi

	local mysql_cmd=$(get_mysql_command)

    local current_buffer_gb=$("$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" -e "SELECT ROUND(@@innodb_buffer_pool_size/1024/1024/1024, 2);" 2>/dev/null | tail -n +2)
    
    if (( $(echo "$target_buffer_gb > $current_buffer_gb" | bc -l 2>/dev/null || echo "0") )); then
        log_info "Temporarily increasing buffer pool from ${current_buffer_gb}GB to ${target_buffer_gb}GB for import"
        
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
        
        if [[ -n "$config_file" ]]; then
            local backup_file="${config_file}.temp_import.$(date +%Y%m%d_%H%M%S)"
            sudo cp "$config_file" "$backup_file"
            
            sudo sed -i '/^innodb_buffer_pool_size/d' "$config_file"
            if grep -q "^\[mysqld\]" "$config_file"; then
                sudo sed -i '/^\[mysqld\]/a innodb_buffer_pool_size = '"${target_buffer_gb}G" "$config_file"
            else
                echo -e "\n[mysqld]\ninnodb_buffer_pool_size = ${target_buffer_gb}G" | sudo tee -a "$config_file" >/dev/null
            fi
            
            log_info "Restarting MariaDB with optimized buffer pool..."
            sudo systemctl restart mariadb
            sleep 3
            
            echo "$backup_file" > "$BASE_DIR/temp_config_backup"
            return 0
        fi
    fi
    
    return 1
}

restore_original_buffer_config() {
    local backup_file_location="$BASE_DIR/temp_config_backup"
    
    if [[ -f "$backup_file_location" ]]; then
        local backup_file=$(cat "$backup_file_location")
        if [[ -f "$backup_file" ]]; then
            log_info "Restoring original buffer pool configuration..."
            sudo cp "$backup_file" "${backup_file%.temp_import.*}"
            sudo systemctl restart mariadb
            sleep 3
            sudo rm -f "$backup_file" "$backup_file_location"
            log_success "Original configuration restored"
        fi
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
	test_db_connection
	
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
	for table in $tables; do
		if [[ -n "$table" ]]; then
			log_progress "Dumping table: $table"
			if "$mysqldump_cmd" -u "$DB_USER" -p"$DB_PASS" \
				--single-transaction --skip-lock-tables \
				"$DB_NAME" "$table" > "$DUMP_DIR/$table.sql" 2>/dev/null; then
				exported=$((exported + 1))
			else
				log_warning "Failed to dump table: $table"
			fi
		fi
	done

	log_success "Successfully exported $exported/$table_count tables"
	
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
	# else
	# 	read -p "$(echo -e "${YELLOW}Delete temporary dump files? [y/N]:${NC} ")" -n 1 -r
	# 	echo
	# 	if [[ $REPLY =~ ^[Yy]$ ]]; then
	# 		rm -rf "$DUMP_DIR"/*
	# 		rm -f "$ERROR_REPORT" "$SUCCESS_LOG"
	# 		rm -rf "$ERROR_LOG_DIR"/*
	# 		log_success "Temporary files cleaned up"
	# 	fi
	fi
}

# === IMPORT FUNCTION ===
import_database() {
	local archive_file="$1"
	local auto_cleanup="$2"

	local cleanup_buffer=false
	trap 'if [[ "${cleanup_buffer:-false}" == "true" ]]; then restore_original_buffer_config; fi' EXIT
	
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
	test_db_connection

	local buffer_info=$(get_optimal_buffer_size)
	local current_buffer_gb=$(echo "$buffer_info" | cut -d: -f1)
	local suggested_buffer_gb=$(echo "$buffer_info" | cut -d: -f2) 
	local total_ram_gb=$(echo "$buffer_info" | cut -d: -f3)

	local mysql_cmd=$(get_mysql_command)
	
	# Detailed system info
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
			# Auto mode
			if apply_temporary_buffer_optimization "$suggested_buffer_gb"; then
				use_buffer_optimization=true
				cleanup_buffer=true
			fi
		else
			# Interactive mode
			read -p "$(echo -e "${YELLOW}Temporarily increase buffer pool to ${suggested_buffer_gb}GB for faster import? [Y/n]:${NC} ")" -n 1 -r
			echo
			if [[ ! $REPLY =~ ^[Nn]$ ]]; then
				if apply_temporary_buffer_optimization "$suggested_buffer_gb"; then
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
	log_success "Using extraction directory: $EXTRACT_DIR"
	
	log_step "Extracting archive: $archive_file"
	if tar -xzf "$archive_file" -C "$EXTRACT_DIR" 2>/dev/null; then
		local file_count=$(find "$EXTRACT_DIR" -name "*.sql" | wc -l)
		log_success "Extracted $file_count SQL files"
	else
		log_error "Failed to extract archive"
		exit 1
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

	import_single_file() {
		local sql_file="$1"
		local filename=$(basename "$sql_file")
		local mysql_cmd="$2"
		local error_log="$ERROR_LOG_DIR/${filename}.error"
		local max_retries=3
		local retry=0
		
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

	export -f import_single_file
	export DB_USER DB_PASS DB_NAME ERROR_LOG_DIR ERROR_REPORT SUCCESS_LOG

	local import_success=false
	if parallel -j "$threads" import_single_file {} "$mysql_cmd" ::: "$EXTRACT_DIR"/*.sql; then
		local success_count=$(wc -l < "$SUCCESS_LOG" 2>/dev/null || echo "0")
		log_success "All $success_count SQL files imported successfully"
		import_success=true
	else
		local success_count=$(wc -l < "$SUCCESS_LOG" 2>/dev/null || echo "0")
		local total_files=$(find "$EXTRACT_DIR" -name "*.sql" | wc -l)
		local failed_count=$((total_files - success_count))
		
		log_warning "$failed_count out of $total_files imports failed during parallel phase"
		log_info "Will retry failed imports sequentially.."
		
		if retry_failed_imports "$mysql_cmd"; then
			log_success "Failed imports successfully retried"
			import_success=true
		else
			log_error "Some imports still failed after retry attempts"
			import_success=false
		fi
	fi
	
	if [[ "$auto_cleanup" == "true" ]]; then
		rm -rf "$EXTRACT_DIR"/*
		rm -f "$ERROR_REPORT" "$SUCCESS_LOG"
		rm -rf "$ERROR_LOG_DIR"/*
		log_success "Temporary files automatically cleaned up"
	fi
	
	if [[ "$use_buffer_optimization" == "true" ]]; then
		sudo -v 2>/dev/null || sudo -v
		restore_original_buffer_config
	fi
	
	if [[ "$import_success" == "true" ]]; then
		log_success "Database import completed successfully!"
	else
		log_error "Database import completed with errors!"
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
	
	"$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" << 'EOF'
SET SESSION foreign_key_checks = 0;
SET SESSION unique_checks = 0;
SET SESSION autocommit = 0;
SET SESSION innodb_lock_wait_timeout = 600;  -- 10 minutes for big tables
EOF
	
	local failed_files=()
	while IFS= read -r line; do
		if [[ "$line" =~ ^FAILED:\ (.+)\.sql ]]; then
			local table_name="${BASH_REMATCH[1]%% *}"
			local sql_file="$EXTRACT_DIR/${table_name}.sql"
			if [[ -f "$sql_file" ]]; then
				failed_files+=("$sql_file")
			fi
		fi
	done < "$ERROR_REPORT"
	
	if [[ ${#failed_files[@]} -eq 0 ]]; then
		log_warning "No valid failed files found to retry"
		return 0
	fi
	
	log_info "Retrying ${#failed_files[@]} failed tables sequentially.."
	
	local retry_success=0
	local retry_failed=0
	
	for sql_file in "${failed_files[@]}"; do
		local table_name=$(basename "$sql_file" .sql)
		local file_size_mb=$(( $(stat -c%s "$sql_file" 2>/dev/null || stat -f%z "$sql_file" 2>/dev/null || echo 0) / 1024 / 1024 ))
		
		log_progress "Retrying: $table_name (${file_size_mb}MB)"
		
		local start_time=$(date +%s)
		if "$mysql_cmd" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$sql_file" 2>/dev/null; then
			local end_time=$(date +%s)
			local duration=$((end_time - start_time))
			log_success "$table_name completed (${duration}s)"
			((retry_success++))
		else
			log_error "Still failed: $table_name"
			((retry_failed++))
		fi
	done
	
	if [[ $retry_failed -eq 0 ]]; then
		log_success "All failed imports successfully retried! ($retry_success/$retry_success)"
	elif [[ $retry_success -gt 0 ]]; then
		log_warning "Partial success: $retry_success succeeded, $retry_failed still failed"
		log_info "Check remaining failures manually"
	else
		log_error "All retry attempts failed ($retry_failed/$retry_failed)"
		return 1
	fi
	
	return 0
}


# === CONFIGURATION FUNCTION ===
configure_database() {
	echo -e "${WHITE}Current Configuration:${NC}"
	echo -e "  Database Name: ${CYAN}$DB_NAME${NC}"
	echo -e "  Database User: ${CYAN}$DB_USER${NC}"
	echo -e "  Database Pass: ${CYAN}***${NC}"
	echo
	
	read -p "$(echo -e "${YELLOW}Change database name? [y/N]:${NC} ")" -n 1 -r
	echo
	if [[ $REPLY =~ ^[Yy]$ ]]; then
		read -p "Enter database name: " DB_NAME
	fi
	
	read -p "$(echo -e "${YELLOW}Change database user? [y/N]:${NC} ")" -n 1 -r
	echo
	if [[ $REPLY =~ ^[Yy]$ ]]; then
		read -p "Enter database user: " DB_USER
	fi
	
	read -p "$(echo -e "${YELLOW}Change database password? [y/N]:${NC} ")" -n 1 -r
	echo
	if [[ $REPLY =~ ^[Yy]$ ]]; then
		read -s -p "Enter database password: " DB_PASS
		echo
	fi
	
	log_success "Configuration updated"
}


# === USAGE FUNCTION ===
show_usage() {
	echo -e "${WHITE}MariaDB Import/Export Tool${NC}"
	echo -e "${WHITE}Usage:${NC}"
	echo "  $0 -e                        # Export database to compressed archive"
	echo "  $0 -i <archive.tar.gz>       # Import database from archive (with buffer optimization)"
	echo "  $0 -c                        # Configure database connection"
	echo "  $0 -d -e                     # Export with automatic cleanup"
	echo "  $0 -d -i <archive.tar.gz>    # Import with auto-cleanup and auto-optimization"
	echo "  $0 --help                    # Show this help message"
	echo
	echo -e "${WHITE}Examples:${NC}"
	echo "  $0 -e"
	echo "  $0 -i db_dump_DB_A_20231201_143022.tar.gz"
	echo "  $0 -d -e                     # Export and auto-delete temp files"
	echo "  $0 -d -i backup.tar.gz       # Import with auto-optimization and cleanup"
	echo "  $0 -c"
	echo
	echo -e "${WHITE}Import Features:${NC}"
	echo "  - Automatic buffer pool optimization for faster imports"
	echo "  - Adaptive sizing based on available system RAM"
	echo "  - Temporary configuration (restored after import)"
	echo "  - Interactive mode asks permission, auto mode (-d) applies automatically"
	echo
	echo -e "${WHITE}Directories:${NC}"
	echo "  Base: ~/database_operations/"
	echo "  Dumps: ~/database_operations/dumps/"
	echo "  Restore: ~/database_operations/restore/"
}

# === MAIN SCRIPT ===
main() {
	if [[ $EUID -eq 0 ]]; then
		log_warning "Running as root. This is not recommended for database operations."
	fi
	
	# Parse for auto-cleanup flag
	local auto_cleanup=false
	local args=()
	
	for arg in "$@"; do
		if [[ "$arg" == "-d" ]]; then
			auto_cleanup=true
		else
			args+=("$arg")
		fi
	done
	
	case "${args[0]:-}" in
		-e|export)
			export_database "$auto_cleanup"
			;;
		-i|import)
			import_database "${args[1]:-}" "$auto_cleanup"
			;;
		-c|config)
			configure_database
			;;
		--help|-h|help)
			show_usage
			;;
		--test-buffer)
			test_larger_buffer_pool
			;;
		--verify-buffer)
			verify_buffer_pool_change
			;;
		--test-import-buffer):
			test_import_with_buffer_optimization "${args[1]:-}"
			;;
		*)
			log_error "Invalid option: ${args[0]:-}"
			show_usage
			exit 1
			;;
	esac
}

main "$@"
