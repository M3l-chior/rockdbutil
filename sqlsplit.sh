#!/bin/bash

# ===============================================
# sqlsplit.sh
# Smart SQL dump splitter for rockdbutil
#
# Splits a monolithic mysqldump/mariadb-dump .sql.gz
# into per-table .sql files.
#
# Standalone usage (produces a tar.gz for manual import):
#   ./sqlsplit.sh <dump.sql.gz>
#   ./sqlsplit.sh <dump.sql.gz> --output /path/to/dir
#
# Internal usage (called by rockdbutil - writes directly into EXTRACT_DIR):
#   ./sqlsplit.sh <dump.sql.gz> --direct-to-dir /path/to/extract_dir
#
# Copyright (c) 2025
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
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BASE_WORK_DIR="${TMPDIR:-/tmp}/sqlsplit_$$"
OUTPUT_DIR=""
DIRECT_TO_DIR=""
AUTO_CLEANUP=false
DB_NAME_OVERRIDE=""
LARGE_TABLE_THRESHOLD_MB=300

# === USAGE ===
show_usage() {
	echo -e "${WHITE}sqlsplit - Smart SQL dump splitter for rockdbutil${NC}"
	echo
	echo -e "${WHITE}Usage:${NC}"
	echo "  $0 <dump.sql.gz> [options]"
	echo
	echo -e "${WHITE}Options:${NC}"
	echo "  --output DIR          Output directory for the generated tar.gz (default: current dir)"
	echo "  --direct-to-dir DIR   Write per-table .sql files directly into DIR (no tar.gz packaging)"
	echo "                        Used internally by rockdbutil - skips the archive step entirely"
	echo "  --db-name NAME        Override database name (skips detection from dump header)"
	echo "                        Passed automatically by rockdbutil from the loaded profile"
	echo "  --cleanup             Remove temporary working files after completion"
	echo "  -h, --help            Show this help"
	echo
	echo -e "${WHITE}Examples:${NC}"
	echo "  $0 db_backup.sql.gz"
	echo "  $0 db_backup.sql.gz --output ~/splits/"
	echo "  $0 db_backup.sql.gz --direct-to-dir ~/database_operations/restore"
}

# === DEPENDENCY CHECKS ===
check_dependencies() {
	local missing=()

	command -v awk  &>/dev/null || missing+=("awk")
	command -v gzip &>/dev/null || missing+=("gzip")
	command -v tar  &>/dev/null || missing+=("tar")
	command -v zcat &>/dev/null || missing+=("zcat (part of gzip)")

	if [[ ${#missing[@]} -gt 0 ]]; then
		log_error "Missing required tools: ${missing[*]}"
		exit 1
	fi
}

# === DETECT DATABASE NAME FROM DUMP ===
# mysqldump embeds the DB name in comments - scan only the header to find it fast
detect_db_name() {
	local gz_file="$1"

	local db_name
	db_name=$(
		zcat "$gz_file" 2>/dev/null \
		| head -n 200 \
		| awk '
			/^-- Current Database:/ {
				line = $0
				gsub(/^[^`]*`/, "", line)
				gsub(/`.*$/, "", line)
				if (line != "") { print line; exit }
			}
			/^USE `/ {
				line = $0
				gsub(/^[^`]*`/, "", line)
				gsub(/`.*$/, "", line)
				if (line != "") { print line; exit }
			}
		'
	)

	if [[ -z "$db_name" ]]; then
		db_name=$(basename "$gz_file" .sql.gz)
		db_name="${db_name//_backup/}"
		db_name="${db_name//_dump/}"
		db_name="${db_name//_export/}"
		log_warning "Could not detect database name from dump header, using: $db_name"
	fi

	echo "$db_name"
}

# === EXTRACT GLOBAL PREAMBLE ===
# Captures SET statements and charset config that must prefix every table file
extract_preamble() {
	local gz_file="$1"
	local preamble_file="$2"
	local sequences_file="${3:-}"

	# || true: zcat returns SIGPIPE (141) when awk exits early on large files.
	# Under set -euo pipefail this would be treated as a fatal error - suppress it.
	zcat "$gz_file" 2>/dev/null \
	| awk -v preamble="$preamble_file" -v sequences="$sequences_file" '
		/^-- Table structure for table/ { exit }
		/^CREATE TABLE/                 { exit }

		# Capture CREATE SEQUENCE blocks into sequences file.
		# Rewrite as CREATE OR REPLACE SEQUENCE so re-runs are idempotent.
		/^CREATE (OR REPLACE )?SEQUENCE/ {
			in_sequence = 1
			if (sequences != "") {
				line = $0
				sub(/^CREATE SEQUENCE/, "CREATE OR REPLACE SEQUENCE", line)
				print line >> sequences
			}
			next
		}

		in_sequence {
			if (sequences != "") print >> sequences
			if (/;[[:space:]]*$/ || /^(CREATE|DROP|ALTER|INSERT|LOCK|UNLOCK|USE|SET|DO|--)[[:space:]]/) in_sequence = 0
			next
		}

		# DO SETVAL(...) initialises sequence values - must go after CREATE SEQUENCE,
		# so route these to the sequences file too, not the per-table preamble.
		/^DO (SETVAL|setval)\(/ {
			if (sequences != "") print >> sequences
			next
		}

		{ print >> preamble }
	' || true
}

# === CORE SPLIT LOGIC ===
# Streams zcat output through awk - never writes the full uncompressed SQL to disk.
#
# awk state machine:
#   - "-- Table structure for table `name`" opens a new per-table file
#   - Prepends the global preamble + transaction wrapper to each file
#   - Closes and finalises the previous file before opening the next
#   - "-- Dumping data for table `name`" is written as a marker (no new file opened)
#   - Writes final table count to count_file (not stdout) to avoid $() subshell usage
split_sql_by_table() {
	local gz_file="$1"
	local tables_dir="$2"
	local preamble_file="$3"
	local count_file="$4"
	local threshold_mb="${5:-300}"

	log_step "Streaming and splitting SQL dump.."

	# || true: zcat returns SIGPIPE (141) when the dump is fully consumed mid-stream.
	# Under set -euo pipefail this would be treated as a fatal error - suppress it.
	zcat "$gz_file" 2>/dev/null \
	| awk -v tables_dir="$tables_dir" -v preamble="$preamble_file" -v count_file="$count_file" \
	      -v threshold_bytes="$((threshold_mb * 1024 * 1024))" '
		function open_table_file(name,    path) {
			if (current_file != "") {
				print "COMMIT;" >> current_file
				close(current_file)
			}
			path = tables_dir "/" name ".sql"
			current_file = path
			current_bytes = 0

			while ((getline line < preamble) > 0) {
				print line >> current_file
				current_bytes += length(line) + 1
			}
			close(preamble)

			print "-- sqlsplit: table " name >> current_file
			print "SET foreign_key_checks = 0;" >> current_file
			print "SET unique_checks = 0;" >> current_file
			print "SET autocommit = 0;" >> current_file
			print "START TRANSACTION;" >> current_file
			table_count++
		}

		BEGIN {
			current_file = ""
			current_table = ""
			current_bytes = 0
			table_count = 0
			large_tables = ""
		}

		/^-- Table structure for table `/ {
			# Record size of previous table before opening next
			if (current_table != "" && current_bytes >= threshold_bytes) {
				large_tables = large_tables current_table "\n"
			}
			line = $0
			gsub(/^[^`]*`/, "", line)
			gsub(/`.*$/, "", line)
			if (line != "") {
				current_table = line
				open_table_file(current_table)
			}
			next
		}

		/^-- Dumping data for table `/ {
			if (current_file != "") {
				print $0 >> current_file
				current_bytes += length($0) + 1
			}
			next
		}

		/^-- Dump completed/ { next }

		{
			if (current_file != "") {
				print >> current_file
				current_bytes += length($0) + 1
			}
		}

		END {
			if (current_file != "") {
				# Check last table
				if (current_table != "" && current_bytes >= threshold_bytes) {
					large_tables = large_tables current_table "\n"
				}
				print "COMMIT;" >> current_file
				close(current_file)
			}
			print table_count > count_file
			# Write large tables manifest alongside the sql files
			if (large_tables != "") {
				manifest = tables_dir "/__large_tables.txt"
				printf "%s", large_tables > manifest
				close(manifest)
			}
		}
	' || true
}

# === CLEANUP ===
cleanup_work_dir() {
	if [[ -d "$BASE_WORK_DIR" ]]; then
		rm -rf "$BASE_WORK_DIR"
		log_info "Cleaned up working directory"
	fi
}

# === MAIN ===
main() {
	if [[ $# -eq 0 ]]; then
		show_usage
		exit 1
	fi

	local input_file="$1"
	shift

	while [[ $# -gt 0 ]]; do
		case "$1" in
			--output)
				OUTPUT_DIR="$2"
				shift 2
				;;
			--direct-to-dir)
				DIRECT_TO_DIR="$2"
				shift 2
				;;
			--db-name)
				DB_NAME_OVERRIDE="$2"
				shift 2
				;;
			--threshold)
				LARGE_TABLE_THRESHOLD_MB="$2"
				shift 2
				;;
			--cleanup)
				AUTO_CLEANUP=true
				shift
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

	if [[ ! -f "$input_file" ]]; then
		log_error "Input file not found: $input_file"
		exit 1
	fi

	if [[ "$input_file" != *.gz ]]; then
		log_error "Expected a .gz compressed file (e.g. db_backup.sql.gz)"
		log_info "If your dump is uncompressed, run: gzip -k yourfile.sql"
		exit 1
	fi

	check_dependencies

	log_step "sqlsplit starting - input: $(basename "$input_file")"

	# BASE_WORK_DIR must exist before anything tries to write into it (preamble, count file).
	# Create it first, before setting up tables_dir which may point elsewhere in --direct-to-dir mode.
	mkdir -p "$BASE_WORK_DIR"

	local preamble_file="$BASE_WORK_DIR/preamble.sql"
	local count_file="$BASE_WORK_DIR/table_count.txt"

	# In --direct-to-dir mode, table .sql files go straight into the caller-supplied
	# directory. The preamble and count files still live in BASE_WORK_DIR.
	local tables_dir
	if [[ -n "$DIRECT_TO_DIR" ]]; then
		tables_dir="$DIRECT_TO_DIR"
		mkdir -p "$tables_dir"
	else
		tables_dir="$BASE_WORK_DIR/tables"
		mkdir -p "$tables_dir"
	fi

	trap 'cleanup_work_dir' EXIT

	# Use the override DB name when provided (passed by rockdbutil from the loaded profile).
	# Only fall back to dump header detection when running standalone.
	# detect_db_name uses $() internally but does not touch BASE_WORK_DIR - safe.
	local db_name
	if [[ -n "$DB_NAME_OVERRIDE" ]]; then
		db_name="$DB_NAME_OVERRIDE"
		log_info "Database: $db_name (from profile)"
	else
		log_step "Detecting database name.."
		db_name=$(detect_db_name "$input_file")
		log_info "Database: $db_name"
	fi

	log_step "Extracting global preamble..."
	local sequences_file="$tables_dir/__sequences.sql"
	extract_preamble "$input_file" "$preamble_file" "$sequences_file"

	local preamble_lines
	preamble_lines=$(wc -l < "$preamble_file")
	log_info "Captured global preamble: ${preamble_lines} lines"

	if [[ -f "$sequences_file" && -s "$sequences_file" ]]; then
		local seq_count
		seq_count=$(grep -c "^CREATE OR REPLACE SEQUENCE" "$sequences_file" || true)
		log_info "Extracted ${seq_count} SEQUENCE definition(s) for pre-import"
	fi

	# Disarm EXIT trap before the split so it does not fire when this script
	# exits normally after returning to rockdbutil. BASE_WORK_DIR is cleaned
	# up explicitly below once the split has fully completed.
	trap - EXIT

	# table_count is written to count_file by awk - avoids $() subshell which
	# would cause the EXIT trap to fire mid-split and wipe BASE_WORK_DIR.
	split_sql_by_table "$input_file" "$tables_dir" "$preamble_file" "$count_file" "$LARGE_TABLE_THRESHOLD_MB"

	local table_count=0
	if [[ -f "$count_file" ]]; then
		table_count=$(cat "$count_file")
	fi

	if [[ -z "$table_count" || "$table_count" -eq 0 ]]; then
		log_error "No tables detected in dump. Is this a valid mysqldump/mariadb-dump file?"
		log_info "Expected markers like: -- Table structure for table \`tablename\`"
		cleanup_work_dir
		exit 1
	fi

	log_success "Split complete: ${table_count} tables"

	local manifest="$tables_dir/__large_tables.txt"
	if [[ -f "$manifest" ]]; then
		local large_count
		large_count=$(wc -l < "$manifest")
		log_info "Large tables flagged for chunked import: ${large_count} (>${LARGE_TABLE_THRESHOLD_MB}MB)"
	fi

	# --direct-to-dir: files are already in place - rockdbutil handles the import.
	if [[ -n "$DIRECT_TO_DIR" ]]; then
		log_info "Files written directly to: $DIRECT_TO_DIR"
		cleanup_work_dir
		return 0
	fi

	# Standalone mode: package into a tar.gz the user can import manually
	if [[ -z "$OUTPUT_DIR" ]]; then
		OUTPUT_DIR="$(pwd)"
	fi
	mkdir -p "$OUTPUT_DIR"

	local archive_name="db_dump_${db_name}_${TIMESTAMP}.tar.gz"
	local archive_path="${OUTPUT_DIR}/${archive_name}"

	log_step "Packaging per-table SQL files into archive.."

	local file_count
	file_count=$(find "$tables_dir" -maxdepth 1 -name "*.sql" | wc -l)
	log_info "Packing ${file_count} table files"

	tar -czf "$archive_path" -C "$tables_dir" .

	local archive_size_mb
	archive_size_mb=$(( $(stat -c%s "$archive_path" 2>/dev/null || stat -f%z "$archive_path") / 1024 / 1024 ))
	log_success "Archive created: $archive_name (${archive_size_mb}MB, ${file_count} tables)"

	cleanup_work_dir

	echo
	log_success "Ready to import with:"
	echo -e "  ${CYAN}./rockdbutil.sh -i ${archive_path}${NC}"
}

main "$@"
