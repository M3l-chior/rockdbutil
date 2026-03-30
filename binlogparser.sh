#!/bin/bash

# ===============================================
# binlogparser.sh
# Parses MariaDB binary logs to extract tables
# that had DML changes (INSERT/UPDATE/DELETE)
# between two points in time.
#
# Reads binlogs over the MariaDB connection using
# --read-from-remote-server - no direct file access
# or system-level permissions required.
#
# Standalone usage:
#   ./binlogparser.sh --since "2025-03-27 13:00:00"
#   ./binlogparser.sh --since "2025-03-27 13:00:00" --until "2025-03-27 18:00:00"
#   ./binlogparser.sh --from-pos mariadb-bin.000001:330
#
# Pipe directly to rockdbutil (future --selective-restore flag):
#   ./binlogparser.sh --since "2025-03-27 13:00:00" --pipe-to-rockdbutil
#
# Required database privileges:
#   GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'user'@'host';
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

# === LOGGING ===
log_info()     { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success()  { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning()  { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error()    { echo -e "${RED}[ERROR]${NC} $1" >&2; }
log_step()     { echo -e "${PURPLE}[STEP]${NC} $1"; }
log_progress() { echo -e "${CYAN}[PROGRESS]${NC} $1"; }

# === DEFAULTS ===
DB_USER=""
DB_PASS=""
DB_HOST="localhost"
DB_PORT="3306"
DB_NAME=""
SINCE_DATETIME=""
UNTIL_DATETIME=""
FROM_POS=""
OUTPUT_FILE=""
PIPE_TO_ROCKDBUTIL=false
FROM_EXPORT_FILE=""
ROCKDBUTIL_PATH="$(dirname "$(realpath "$0")")/rockdbutil.sh"
CONFIG_FILE="$HOME/.config/rockdbutil.conf"
DB_PROFILE="default"

# === USAGE ===
show_usage() {
	echo -e "${WHITE}binlogparser - MariaDB binlog table change detector${NC}"
	echo
	echo -e "${WHITE}Usage:${NC}"
	echo "  $0 [options]"
	echo
	echo -e "${WHITE}Time range options (one required):${NC}"
	echo "  --since DATETIME        Start datetime e.g. '2025-03-27 13:00:00'"
	echo "  --until DATETIME        End datetime (default: now)"
	echo "  --from-pos FILE:POS     Start from a specific binlog file and position"
	echo "                          e.g. mariadb-bin.000001:330"
	echo "  --from-export FILE      Read binlog_file and binlog_position from a rockdbutil"
	echo "                          __export_meta.txt file (e.g. last_export_meta.txt)"
	echo
	echo -e "${WHITE}Database options:${NC}"
	echo "  -db, --database PROFILE Load credentials from rockdbutil profile (default: default)"
	echo "  --user USER             Database user (overrides profile)"
	echo "  --pass PASS             Database password (overrides profile)"
	echo "  --host HOST             Database host (overrides profile, default: localhost)"
	echo "  --port PORT             Database port (overrides profile, default: 3306)"
	echo "  --db-name NAME          Filter results to a specific database only"
	echo
	echo -e "${WHITE}Output options:${NC}"
	echo "  --output FILE           Write changed table names to a file (one per line)"
	echo "  --pipe-to-rockdbutil    Pass changed table list directly to rockdbutil"
	echo "                          (requires rockdbutil.sh in the same directory)"
	echo
	echo -e "${WHITE}Examples:${NC}"
	echo "  $0 --since '2025-03-27 13:00:00'"
	echo "  $0 --since '2025-03-27 13:00:00' --until '2025-03-27 18:00:00'"
	echo "  $0 --from-pos mariadb-bin.000001:330 --db-name rocketdb"
	echo "  $0 --from-export ~/database_operations/last_export_meta.txt --db-name rocketdb"
	echo "  $0 --since '2025-03-27 13:00:00' --output changed_tables.txt"
	echo "  $0 --since '2025-03-27 13:00:00' --pipe-to-rockdbutil -db staging"
	echo
	echo -e "${WHITE}Required database privileges:${NC}"
	echo "  GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'user'@'host';"
}

# === GET MYSQL COMMAND ===
get_mysql_command() {
	if command -v mariadb &>/dev/null; then
		echo "mariadb"
	elif command -v mysql &>/dev/null; then
		echo "mysql"
	else
		log_error "No MariaDB/MySQL client found"
		exit 1
	fi
}

# === GET BINLOG COMMAND ===
get_binlog_command() {
	if command -v mariadb-binlog &>/dev/null; then
		echo "mariadb-binlog"
	elif command -v mysqlbinlog &>/dev/null; then
		echo "mysqlbinlog"
	else
		log_error "Neither mariadb-binlog nor mysqlbinlog found"
		log_info "Install with: sudo pacman -S mariadb-clients  or  sudo apt install mariadb-client"
		exit 1
	fi
}

# === LOAD CREDENTIALS FROM ROCKDBUTIL CONFIG ===
load_credentials() {
	local profile="${1:-default}"

	if [[ ! -f "$CONFIG_FILE" ]]; then
		log_error "rockdbutil config not found: $CONFIG_FILE"
		log_info "Either run rockdbutil --setup first, or supply --user and --pass manually"
		exit 1
	fi

	[[ -z "$DB_USER" ]] && DB_USER=$(grep "^${profile}_db_user=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')
	[[ -z "$DB_PASS" ]] && DB_PASS=$(grep "^${profile}_db_pass=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')
	[[ -z "$DB_NAME" ]] && DB_NAME=$(grep "^${profile}_db_name=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')

	local config_host
	local config_port
	config_host=$(grep "^${profile}_db_host=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')
	config_port=$(grep "^${profile}_db_port=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '"')

	[[ -n "$config_host" ]] && DB_HOST="$config_host"
	[[ -n "$config_port" ]] && DB_PORT="$config_port"

	if [[ -z "$DB_USER" || -z "$DB_PASS" ]]; then
		log_error "Could not load credentials for profile: $profile"
		exit 1
	fi

	log_info "Loaded credentials from profile: $profile ($DB_HOST:$DB_PORT)"
}

# === VERIFY BINLOG IS ENABLED AND PRIVILEGES ARE SUFFICIENT ===
check_binlog_enabled() {
	local mysql_cmd
	mysql_cmd=$(get_mysql_command)

	local log_bin
	log_bin=$("$mysql_cmd" -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -sN \
		-e "SELECT @@log_bin;" 2>/dev/null)

	if [[ "$log_bin" != "1" ]]; then
		log_error "Binary logging is not enabled on this server"
		log_info "Add the following to your MariaDB config under [mysqld] and restart:"
		log_info "  log_bin = /var/log/mysql/mariadb-bin"
		log_info "  binlog_format = ROW"
		log_info "  expire_logs_days = 7"
		log_info "  max_binlog_size = 100M"
		exit 1
	fi

	local binlog_format
	binlog_format=$("$mysql_cmd" -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -sN \
		-e "SELECT @@binlog_format;" 2>/dev/null)

	if [[ "$binlog_format" != "ROW" ]]; then
		log_warning "binlog_format is '$binlog_format' - ROW format recommended for reliable table detection"
		log_info "STATEMENT/MIXED mode is supported but may miss some changes"
	fi

	# Verify REPLICATION CLIENT privilege by running SHOW BINARY LOGS
	local binlog_check
	binlog_check=$("$mysql_cmd" -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -sN \
		-e "SHOW BINARY LOGS;" 2>/dev/null | wc -l)

	if [[ "$binlog_check" -eq 0 ]]; then
		log_error "User '$DB_USER' cannot list binary logs - missing REPLICATION CLIENT privilege"
		log_info "Grant the required privileges:"
		log_info "  GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '${DB_USER}'@'${DB_HOST}';"
		log_info "  FLUSH PRIVILEGES;"
		exit 1
	fi

	log_info "Binlog enabled - format: $binlog_format"
}

# === GET LIST OF BINLOG FILES FROM SERVER ===
get_binlog_files() {
	local mysql_cmd
	mysql_cmd=$(get_mysql_command)

	"$mysql_cmd" -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" -sN \
		-e "SHOW BINARY LOGS;" 2>/dev/null \
		| awk '{print $1}'
}

# === PARSE BINLOGS AND EXTRACT CHANGED TABLES ===
parse_binlogs() {
	local binlog_cmd
	binlog_cmd=$(get_binlog_command)

	log_step "Fetching binlog file list from server.."

	local all_binlogs=()
	while IFS= read -r binlog_name; do
		[[ -z "$binlog_name" ]] && continue
		all_binlogs+=("$binlog_name")
	done < <(get_binlog_files)

	if [[ ${#all_binlogs[@]} -eq 0 ]]; then
		log_error "No binlog files found on server"
		log_info "Verify with: SHOW BINARY LOGS;"
		exit 1
	fi

	log_info "Found ${#all_binlogs[@]} binlog file(s) on server"

	local pos_file=""
	local pos_offset=""

	# If --from-pos was supplied, filter to only files from that point onward
	if [[ -n "$FROM_POS" ]]; then
		pos_file=$(echo "$FROM_POS" | cut -d: -f1)
		pos_offset=$(echo "$FROM_POS" | cut -d: -f2)

		local filtered_binlogs=()
		local found_start=false
		local bf
		for bf in "${all_binlogs[@]}"; do
			[[ "$bf" == "$pos_file" ]] && found_start=true
			[[ "$found_start" == "true" ]] && filtered_binlogs+=("$bf")
		done

		if [[ ${#filtered_binlogs[@]} -eq 0 ]]; then
			log_error "Starting binlog file not found in server list: $pos_file"
			exit 1
		fi

		all_binlogs=("${filtered_binlogs[@]}")
		log_info "Scanning from $pos_file position $pos_offset (${#all_binlogs[@]} file(s))"
	fi

	log_step "Streaming ${#all_binlogs[@]} binlog file(s) via remote connection.."

	# Build shared args - credentials and remote server connection.
	# --verbose (-v) is required for ROW format to emit ### INSERT/UPDATE/DELETE annotations.
	# Table_map lines are also parsed as a reliable fallback - they appear regardless of
	# verbosity and cover all DML operation types without needing to decode the binary payload.
	local base_args=(
		--read-from-remote-server
		--host="$DB_HOST"
		--port="$DB_PORT"
		--user="$DB_USER"
		--password="$DB_PASS"
		--verbose
	)

	[[ -n "$SINCE_DATETIME" ]] && base_args+=("--start-datetime=${SINCE_DATETIME}")
	[[ -n "$UNTIL_DATETIME" ]] && base_args+=("--stop-datetime=${UNTIL_DATETIME}")
	[[ -n "$pos_offset" ]]     && base_args+=("--start-position=${pos_offset}")

	# Two complementary patterns cover all MariaDB binlog formats:
	#
	# 1. Table_map lines (ROW format, always present for any DML):
	#       Table_map: `rocketdb`.`tbl_policy` mapped to number 133
	#
	# 2. ### annotation lines (ROW format + --verbose, or STATEMENT/MIXED format):
	#       ### INSERT INTO `rocketdb`.`tbl_policy`
	#       ### UPDATE `rocketdb`.`tbl_policy`
	#       ### DELETE FROM `rocketdb`.`tbl_policy`
	#
	# Table_map is the primary signal - it fires once per table per transaction
	# regardless of how many rows were affected, making it ideal for building a
	# deduplicated changed-table list. The ### lines serve as a cross-check and
	# cover STATEMENT/MIXED format where Table_map lines are absent.
	local changed_tables
	changed_tables=$(
		"$binlog_cmd" "${base_args[@]}" "${all_binlogs[@]}" 2>/dev/null \
		| awk -v db_filter="$DB_NAME" '
			/Table_map: `/ {
				line = $0
				if (match(line, /`([^`]+)`\.`([^`]+)`/, arr)) {
					db    = arr[1]
					table = arr[2]
					if (db_filter == "" || db == db_filter) {
						print db "." table
					}
				}
				next
			}
			/^### (INSERT INTO|UPDATE|DELETE FROM) `/ {
				line = $0
				gsub(/^### (INSERT INTO|UPDATE|DELETE FROM) /, "", line)
				if (match(line, /`([^`]+)`\.`([^`]+)`/, arr)) {
					db    = arr[1]
					table = arr[2]
					if (db_filter == "" || db == db_filter) {
						print db "." table
					}
				}
				next
			}
		' \
		| sort -u
	)


	if [[ -z "$changed_tables" ]]; then
		log_warning "No DML changes detected in the specified time range"
		exit 0
	fi

	local table_count
	table_count=$(echo "$changed_tables" | wc -l)
	log_success "Found $table_count table(s) with changes"

	echo "$changed_tables"
}

# === OUTPUT RESULTS ===
output_results() {
	local changed_tables="$1"

	# Raw table list always goes to stdout - clean for subprocess capture by rockdbutil.
	# All human-readable display goes to stderr so it never pollutes stdout capture.
	echo "$changed_tables"

	echo >&2
	echo -e "${WHITE}Changed tables:${NC}" >&2
	echo "$changed_tables" | while IFS= read -r table; do
		echo -e "  ${CYAN}${table}${NC}" >&2
	done
	echo >&2

	if [[ -n "$OUTPUT_FILE" ]]; then
		echo "$changed_tables" > "$OUTPUT_FILE"
		log_success "Table list written to: $OUTPUT_FILE"
	fi

	if [[ "$PIPE_TO_ROCKDBUTIL" == "true" ]]; then
		if [[ ! -f "$ROCKDBUTIL_PATH" ]]; then
			log_error "rockdbutil.sh not found at: $ROCKDBUTIL_PATH"
			log_info "Place binlogparser.sh in the same directory as rockdbutil.sh"
			exit 1
		fi

		local table_names_only
		table_names_only=$(echo "$changed_tables" | awk -F. '{print $NF}')

		log_step "Passing $(echo "$table_names_only" | wc -l) table(s) to rockdbutil.."
		bash "$ROCKDBUTIL_PATH" --selective-restore \
			--tables "$table_names_only" \
			-db "$DB_PROFILE"
	fi
}

# === MAIN ===
main() {
	if [[ $# -eq 0 ]]; then
		show_usage
		exit 1
	fi

	while [[ $# -gt 0 ]]; do
		case "$1" in
			--since)
				SINCE_DATETIME="$2"
				shift 2
				;;
			--until)
				UNTIL_DATETIME="$2"
				shift 2
				;;
			--from-pos)
				FROM_POS="$2"
				shift 2
				;;
			-db|--database)
				DB_PROFILE="$2"
				shift 2
				;;
			--user)
				DB_USER="$2"
				shift 2
				;;
			--pass)
				DB_PASS="$2"
				shift 2
				;;
			--host)
				DB_HOST="$2"
				shift 2
				;;
			--port)
				DB_PORT="$2"
				shift 2
				;;
			--db-name)
				DB_NAME="$2"
				shift 2
				;;
			--output)
				OUTPUT_FILE="$2"
				shift 2
				;;
			--pipe-to-rockdbutil)
				PIPE_TO_ROCKDBUTIL=true
				shift
				;;
			--from-export)
				FROM_EXPORT_FILE="$2"
				shift 2
				;;
			-h|--help)
				show_usage
				exit 0
				;;
			*)
				log_error "Unknown option: $1"
				show_usage
				exit 1
				;;
		esac
	done

	# Resolve --from-export into FROM_POS before the validation guard.
	# Reads binlog_file and binlog_position from a rockdbutil __export_meta.txt file.
	if [[ -n "$FROM_EXPORT_FILE" ]]; then
		if [[ ! -f "$FROM_EXPORT_FILE" ]]; then
			log_error "--from-export file not found: $FROM_EXPORT_FILE"
			exit 1
		fi

		local meta_binlog_file
		local meta_binlog_position
		meta_binlog_file=$(grep "^binlog_file=" "$FROM_EXPORT_FILE" | cut -d'=' -f2-)
		meta_binlog_position=$(grep "^binlog_position=" "$FROM_EXPORT_FILE" | cut -d'=' -f2-)

		if [[ -z "$meta_binlog_file" || -z "$meta_binlog_position" ]]; then
			log_error "Could not read binlog_file or binlog_position from: $FROM_EXPORT_FILE"
			log_info "File must contain lines: binlog_file=... and binlog_position=..."
			exit 1
		fi

		FROM_POS="${meta_binlog_file}:${meta_binlog_position}"
		log_info "Loaded binlog position from export metadata: $FROM_POS"

		# Also load db_name from meta if not already supplied via --db-name
		if [[ -z "$DB_NAME" ]]; then
			local meta_db_name
			meta_db_name=$(grep "^db_name=" "$FROM_EXPORT_FILE" | cut -d'=' -f2-)
			[[ -n "$meta_db_name" ]] && DB_NAME="$meta_db_name"
		fi
	fi

	if [[ -z "$SINCE_DATETIME" && -z "$FROM_POS" ]]; then
		log_error "A time range is required - supply --since, --from-pos, or --from-export"
		show_usage
		exit 1
	fi

	load_credentials "$DB_PROFILE"
	check_binlog_enabled

	local changed_tables
	changed_tables=$(parse_binlogs)

	output_results "$changed_tables"
}

main "$@"
