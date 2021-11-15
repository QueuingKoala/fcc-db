#!/bin/sh
# shellcheck disable=SC3043

# This file is part of the fcc-db ULS Amateur import project.
#
# Copyright 2021, Josh Cepek
#
# This project is available under the GPLv3 license: see LICENSE.md.

warn() { printf "%s\n" "$@" >&2; }
die() { warn "$@"; exit 1; }

usage_exit() {
	cat <<_EOF
SYNOPSIS:

$0
  [-b BASEDIR] [-m] [-i INFODIR] [-z ZIPDIR] [-t TMPBASE] [-A|-L]

USAGE:

Either -b or all of -i, -z, and -t must to be defined.

With -b, all the info, zip, & tempdir bits go here. If any of -i, -z, or -t
are defined as relative paths, they're relative to BASEDIR or the working
directory without BASEDIR. Absolute paths starting with / are unmodified.

The TMPBASE is the parent of an ephemeral dir is created within, for ULS
downloads. These are compared to existing downloads and zip archives fetched
if updates are available. The ephemeral dir is cleaned up on exit.

-A or -L are optional, and limit the operation to only Apps (-A) or Licenses
(-L) but using both is an error as it would be a no-op.

-m permits the INFO, ZIP, or TMPBASE dirs to be created if missing; BASEDIR
must always exist first when specified. By default it is an error if the 3
required paths are missing, as a measure to avoid re-downloads due to typos.
This option applies only to dirs specified after, allowing selection over
which dirs will be created.

_EOF
	exit 0
}

parse_opts() {
	ULS_APP=defined ULS_LIC=defined
	local missingok=""

	while getopts b:i:z:t:ALmh opt
	do
	  case "$opt" in
		b)
			[ -d "$OPTARG" ] || die "Error: BASEDIR must exist"
			[ "$OPTARG" != "${OPTARG#/}" ] || die "Error: BASEDIR must be absolute"
			BASEDIR="$OPTARG"
			;;
		i)
			[ "$OPTARG" = "${OPTARG#/}" ] \
				&& INFODIR="${BASEDIR:-$PWD}/$OPTARG" \
				|| INFODIR="$OPTARG"
			if [ ! -d "$INFODIR" ]; then
				[ -n "$missingok" ] || die "INFODIR missing: create or use -m"
				mkdir -p "$INFODIR" || die "Failed to create INFODIR"
			fi
			;;
		z)
			[ "$OPTARG" = "${OPTARG#/}" ] \
				&& ZIPDIR="${BASEDIR:-$PWD}/$OPTARG" \
				|| ZIPDIR="$OPTARG"
			if [ ! -d "$ZIPDIR" ]; then
				[ -n "$missingok" ] || die "ZIPDIR missing: create or use -m"
				mkdir -p "$ZIPDIR" || die "Failed to create ZIPDIR"
			fi
			;;
		t)
			[ "$OPTARG" = "${OPTARG#/}" ] \
				&& TMPBASE="${BASEDIR:-$PWD}/$OPTARG" \
				|| TMPBASE="$OPTARG"
			if [ ! -d "$TMPBASE" ]; then
				[ -n "$missingok" ] || die "TMPBASE missing: create or use -m"
				mkdir -p "$TMPBASE" || die "Failed to create TMPBASE"
			fi
			;;
		A)	ULS_LIC="" ;;
		L)	ULS_APP="" ;;
		m)	missingok=defined ;;
		h)	usage_exit ;;
		'?')
			die "Bad arguments"
	  esac
	done

	[ -z "$ULS_APP" ] && [ -z "$ULS_LIC" ] && die "Cannot use both -A and -L at once"
	if [ -n "$BASEDIR" ]; then
		[ -z "$INFODIR" ] && INFODIR="$BASEDIR"
		[ -z "$ZIPDIR" ] && ZIPDIR="$BASEDIR"
		[ -z "$TMPBASE" ] && TMPBASE="$BASEDIR"
	fi
	[ -n "$INFODIR" ] || die "INFODIR or BASEDIR must be defined"
	[ -n "$ZIPDIR" ] || die "ZIPDIR or BASEDIR must be defined"
	[ -n "$TMPBASE" ] || die "TMPBASE or BASEDIR must be defined"
}

main() {
	ULSDIR=$(mktemp -d "$TMPBASE/uls-temp.XXXXXX") \
		|| die "mktemp failed"
	SAVEDPWD="$PWD"
	trap cleanup INT TERM EXIT
	cd "$ULSDIR" || die "failed to cd to ULSDIR: $ULSDIR"

	# Fetch all info-files of non-excluded archive types (LIC/APP)

	days="sat,sun,mon,tue,wed,thu,fri"
	uls_fetch -i \
		${ULS_LIC:+-W l -l "$days"} \
		${ULS_APP:+-W a -a "$days"} \
		|| die "fetch failed for ULS metadata"

	# Iterate info files, comparing past modify times.
	#
	# (glob is quote-safe as these are named under our control.)

	local args="" days_l="" days_a=""
	for fn in *.info; do
		new_mod=$(last_mod "$fn") || {
			warn "Warning: missing Last-Modified header for: $fn"
			continue
		}
		old_mod=$(last_mod "$INFODIR/$fn" 2>/dev/null)
		[ "$old_mod" = "$new_mod" ] && continue

		# New modified time is updated; add to args for fetching.

		al_type="${fn#*_}"
		al_type="${al_type%%_*}"
		kind="${fn%%_*}"
		if [ "$kind" = "weekly" ]; then
			args="$args -W $al_type"
			continue
		fi

		day="${fn##*_}"
		day="${day%.*}"
		case "$al_type" in
			l)	[ -n "$days_l" ] && days_l="$days_l,"
				days_l="$days_l$day" ;;
			a)	[ -n "$days_a" ] && days_a="$days_a,"
				days_a="$days_a$day" ;;
		esac
	done

	# Append nonzero day files to args, then fetch updated zips.

	[ -n "$days_l" ] && args="$args -l $days_l"
	[ -n "$days_a" ] && args="$args -a $days_a"
	[ -z "$args" ] && exit 0

	# Arg word splitting intentional; values all quote-safe
	# shellcheck disable=2086

	uls_fetch $args || die "fetch failed for ULS zips"

	# Iterate new zips, moving each zip/info pair to final dirs.

	local err=0 cnt=0
	for fn in *.zip; do
		cnt=$((cnt+1))
		mv "$fn" "$ZIPDIR/" || err=1
		mv "${fn%.*}.info" "$INFODIR/" || err=1
	done

	if [ $err -eq 0 ]; then
		warn "Success, updated $cnt zips from ULS"
		exit 0
	fi

	warn "WARNING: failed to move some files (check errors above.)"
	die "NOTE: Preserving tmpdir: $ULSDIR"
}

# last_mod() - extract Last-Modified transfer-header from file

last_mod() { grep -i -E '^Last-Modified: ' "$1"; }

uls_fetch() {
	local info="" outs="" urls=""
	#local uri_base="ftp://wirelessftp.fcc.gov/pub/uls"
	local uri_base="https://data.fcc.gov/download/pub/uls/"

	local status="%{filename_effective} %{size_download} %{speed_download} %{url_effective}\n"
	local ext=zip
	OPTIND=1
	while getopts iW:l:a: opt
	do
	  case "$opt" in
		i)
			status="%{filename_effective}\n"
			ext=info
			info=defined
			;;
		W)
			outs="$outs -o weekly_$OPTARG.$ext"
			urls="$urls $uri_base/complete/${OPTARG}_amat.zip"
			;;
		l)
			outs="$outs -o daily_l_#1.$ext"
			urls="$urls $uri_base/daily/l_am_{$OPTARG}.zip"
			;;
		a)
			outs="$outs -o daily_a_#1.$ext"
			urls="$urls $uri_base/daily/a_am_{$OPTARG}.zip"
			;;
		*)	die "Bad uls_fetch() arg"
	  esac
	done

	# These are known quote-safe.
	# shellcheck disable=SC2086

	curl -sS ${info:+-I} --ftp-method nocwd -w "$status" $outs $urls
}

cleanup() {
	# Move to TMPBASE just above ULSDIR, and cleanup. Warn if this or
	# restoring previous working-dir fails.

	if [ -d "$ULSDIR" ] && cd "$TMPBASE"; then
		find "${ULSDIR##*/}" -delete \
			|| warn "Warning: could not cleanup tmp-dir: $ULSDIR"
	elif [ -d "$ULSDIR" ]; then
		warn "Warning: failed to cd to TMPBASE for cleanup of tmp-dir: $ULSDIR"
	fi
	cd "$SAVEDPWD" || warn "Warning: failed to restore working-dir"
}

parse_opts "$@"
main "$@"

