#!/bin/sh

usage_exit() {
	cat <<_END_USAGE
zip-convert: extract, convert, & name dirs for ULS daily zip files

SYNOPSIS:

zip-convert.sh -z <ZIP> -o <OUTDIR>

ARGUMENTS:

<ZIP>: Input zip file
<OUTDIR>: Output directory

_END_USAGE
	exit "${1:-0}"
}

warn() { echo "$1" >&2; }
die() { warn "$1"; exit "${2:-1}"; }

main() {
	outdir="" zip=""
	while getopts ho:z: opt
	do
		case "$opt" in
			h)	usage_exit ;;
			o)	outdir="$OPTARG" ;;
			z)	zip="$OPTARG" ;;
			?)	exit 1
		esac
	done

	[ -r "$zip" ] || die "No zip file: $zip"
	[ -d "$outdir" ] || die "Outdir does not exist: $outdir"

	extdir=$(mktemp -d "$outdir/temp.XXXX") || die "mktemp failed"
	trap cleanup_temp EXIT

	( process_zip ) || die "Processing failed"
	trap - EXIT

	exit 0
}

process_zip() {
	unzip -q "$zip" -d "$extdir" || die "unzip failed"

	# Convert counts & read in date:
	crlf_convert -i "$extdir/counts" -s || die "failed convert counts"
	read -r x1 x2 x3 date < "$extdir/counts"
	[ -n "$date" ] || die "empty date in counts"

	# Format newdir from zip date, and verify it doesn't yet exist:
	newdir="$outdir/$(date -d "$date" +%Y-%m-%d)"
	[ -e "$newdir" ] && die "dir already exists for date: $newdir"

	# Convert the *.dat files to Unix, renaming to *.data
	for dat in "$extdir/"*.dat
	do
		crlf_convert -i "$dat" -o "${dat%.dat}.data" -d || die "failed convert: $dat"
	done

	# Rename the temp dir:
	mv "$extdir" "$newdir" || die "failed final dir rename"

	warn "Successful extract to: $newdir"
	return 0
}

crlf_convert() {
	OPTIND=1
	local in="" out="" tmp="" delete=0
	while getopts i:o:ds opt
	do
		case "$opt" in
			i)	in="$OPTARG" ;;
			o)	out="$OPTARG" ;;
			d)	delete=1 ;;
			s)	
				[ -n "$in" ] || die "Cannot set same output without input given!"
				out="$in"
				;;
			?)	exit 1
		esac
	done
	[ -r "$in" ] || die "CRLF infile does not exist: $in"
	tmp="${in}.crlf-convert"

	perl -ne 'BEGIN {$/="\r\n";} chomp; printf "%s\n", $_;' < "$in" > "$tmp" \
		|| die "CRLF convert failed"

	mv "$tmp" "$out" || die "CRLF tmpfile move failed"

	if [ "$delete" -eq 1 ]; then
		rm "$in" || die "CRLF delete old failed"
	fi

	return 0
}

cleanup_temp() {
	#warn "DEBUG: skipping cleanup temp: $extdir"
	#return 0
	if [ -d "$extdir" ]; then
		rm -rf "$extdir" 2>/dev/null
	fi
}

main "$@"
