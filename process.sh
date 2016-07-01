#!/bin/sh

warn() { echo "$1" >&2; }
die() { warn "$1"; exit "${2:-1}"; }

usage_exit() {
	cat <<_END_USAGE
process: import 1 or more FCC ULS data dirs to a new or existing database

SYNOPSIS:

./daily-import.sh -b <IMPORT_BIN> -d <DATABASE>
	[-w <WEEKLY_DIR> -t <TYPE> [-r] [-n] ]

See also: STDIN

ARGUMENTS:

-b <IMPORT_BIN>: Executable path to import.pl file
-d <DATABASE>: Path to the sqlite database (must already exist)
-w <WEEKLY_DIR>: path to dir for weekly DB import
-t <TYPE>: one of "l[icense]" or "a[pplications]", for new DB schema type
-r: remove an existing database file before a weekly import
-n: no daily dirs passed via STDIN

STDIN:

A list of directories to import may be passed on stdin, one per line.

_END_USAGE
	exit "${2:-0}"
}

main() {
	bin="" db="" remove="" weekly="" db_type="" daily=1
	while getopts hb:d:nrt:w: opt
	do
		case "$opt" in
			h)	usage_exit ;;
			b)	bin="$OPTARG" ;;
			d)	db="$OPTARG" ;;
			n)	daily=0 ;;
			r)	remove=1 ;;
			t)	db_type="$OPTARG" ;;
			w)	weekly="$OPTARG" ;;
			?)	exit 1
		esac
	done

	[ -x "$bin" ] || die "No executable binary passed in -b. Fatal."
	if [ -z "$weekly" ] && [ ! -r "$db" ]; then
		die "No readable db passed in -d. Fatal."
	fi

	# Weekly import, when requested:
	if [ -n "$weekly" ]; then
		# Get schema/index paths based on DB-type:
		schema_dir="$(dirname "$bin")/schema"
		case "$db_type" in
			l*)	schema_pfx="l" ;;
			a*)	schema_pfx="a" ;;
			*)	die "Invalid/missing -t (TYPE) argument: expected 'l'/'a'"
		esac
		schema="$schema_dir/$schema_pfx-am-schema.sql"
		indexes="$schema_dir/$schema_pfx-am-indexes.sql"
		[ -r "$schema" ] || die "Unreadable schema: $schema"
		[ -r "$indexes" ] || die "Unreadable indexes: $indexes"

		call_import "$weekly" ${remove:+-r} -s "$schema" -i "$indexes" \
			|| die "Import failed"
	fi

	# Daily table imports from STDIN:
	if [ "$daily" -eq 1 ]; then
		while IFS= read -r name
		do
			[ -d "$name" ] || die "dir does not exist: $name"
			echo "Importing ${name}.."
			call_import "$name" -u || die "Import failed"
			echo ".. Import Done."
		done
	fi

	# Post-import analyze:
	echo "Calling ANALYZE.."
	echo "ANALYZE;" | sqlite3 "$db" \
		|| die ".. FAILED ANALYZE"
	echo ".. Analyze done."
}

# call_import() - wrapper around import script execution
#
# call_import <IMPORT_DIR> [<OPTS_FOR_IMPORT_SCRIPT> ..]

call_import() {
	local name="$1"
	shift
	[ -d "$name" ] || die "dir does not exist: $name"

	local	pHD="$name/HD.data" \
		pEN="$name/EN.data" \
		pAM="$name/AM.data" \
		pAD="$name/AD.data" \
		pVC="$name/VC.data"

	local tHD="" tEN="" tAM="" tAD="" tVC=""

	[ -r "$pHD" ] && tHD=1
	[ -r "$pEN" ] && tEN=1
	[ -r "$pAM" ] && tAM=1
	[ -r "$pAD" ] && tAD=1
	[ -r "$pVC" ] && tVC=1

	"$bin" -d "$db" --no-analyze "$@" \
		${tHD:+--hd "$pHD"} \
		${tEN:+--en "$pEN"} \
		${tAM:+--am "$pAM"} \
		${tAD:+--ad "$pAD"} \
		${tVC:+--vc "$pVC"} \
		|| die "Import execute failed on: $name"

	return 0
}

main "$@"
