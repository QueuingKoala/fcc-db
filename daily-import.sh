#!/bin/sh

warn() { echo "$1" >&2; }
die() { warn "$1"; exit "${2:-1}"; }

usage_exit() {
	cat <<_END_USAGE
daily-import: import 1 or more FCC ULS data dirs to an existing database

SYNOPSIS:

./daily-import.sh -b <IMPORT_BIN> -d <DATABASE>

See also: STDIN

ARGUMENTS:

<IMPORT_BIN>: Executable path to import.pl file
<DATABASE>: Path to the sqlite database (must already exist)

STDIN:

A list of directories to import must be passed on stdin, one per line.

_END_USAGE
	exit "${2:-0}"
}

main() {
	bin="" db=""
	while getopts hb:d: opt
	do
		case "$opt" in
			h)	usage_exit ;;
			b)	bin="$OPTARG" ;;
			d)	db="$OPTARG" ;;
			?)	exit 1
		esac
	done

	[ -x "$bin" ] || die "No executable binary passed in -b. Fatal."
	[ -r "$db" ] || die "No readable db passed in -d. Fatal."

	# Table imports:
	while IFS= read -r name
	do
		[ -d "$name" ] || die "dir does not exist: $name"
		echo "Importing ${name}.."
		call_import "$name" || die "Import failed"
		echo ".. Import Done."
	done

	# Post-import analyze:
	echo "Calling ANALYZE.."
	echo "ANALYZE;" | sqlite3 "$db" \
		|| die ".. FAILED ANALYZE"
	echo ".. Analyze done."
}

call_import() {
	local name="$1"
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

	"$bin" -d "$db" -u --no-analyze \
		${tHD:+--hd "$pHD"} \
		${tEN:+--en "$pEN"} \
		${tAM:+--am "$pAM"} \
		${tAD:+--ad "$pAD"} \
		${tVC:+--vc "$pVC"} \
		|| die "Import execute failed on: $name"

	return 0
}

main "$@"
