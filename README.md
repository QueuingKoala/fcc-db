REFERENCE TO FCC-DB: ULS AMATEUR LICENSE IMPORTER
=====

Description
---

fcc-db is a project to download and import FCC ULS licensing & application
data for the Amateur Radio service.

Copyright 2016-2021, Josh Cepek

This project is avilable under the GPLv3; see `LICENSE.md` for details.

Quick start guide
---

Please read the ULS Publish Schedule & Timetamp notes below for context.
This quickstart assumes readers know what sequence of daily zips have been
downloaded more recently than the latest weekly to feed to the importer.

## Prereqs

`uls-fetch.sh` requires only `curl` and a POSIX-sh enviornment.

`import.pl` requires:

* 7-zip binary `7z` (often from `7zip` or `p7zip` distro packages)
* Perl 5, and the following perl-libs (via your distro or CPAN)
* DBI
* DBD::SQLite

On Debian-based systems, install the following packages:

* `curl`
* `libconfig-inifiles-perl`
* `libdbd-sqlite3-perl`
* `libdbi-perl`
* `p7zip-full`

## First-use prep

uls-fetch.sh is what downloads ULS archives, both initially and to check for
subsequent updates. It needs a directory to hold zips, info metadata files,
and an ephemeral temp-dir for downloads; all 3 may be the same but need not
be.

Easiest is to create a `BASEDIR` anywhere you like and use this, optionally
with subdirs for zips and info files. If you'd prefer to manually define all
3 locations, see the help output with `-h` to the fetch script. The commands
below assume use of a `BASEDIR` at `/usr/local/var/uls`.

Optionally, copy the `*.ini.sample` files to `*.ini` names and edit paths to
taste. Note that schema/index files will be relative to the invoker's
directory, so absolute paths may be desired if invoked elsewhere. Licensing
& Applicaiton database must be unique, as they function independently.

## Fetching ULS daily files

This process checks ULS metadata and downloads missing or changed archives.
See the `-h` argument for per-dir control or selective archive options.

```
./uls-fetch.sh -b /usr/local/var/uls
```

## Importing weeklies 

To (re)-create SQLite files from weeklies, call import.pl. While manual
options are allowed, use of INI config files are suggested as convenient
shortcuts.

```
./import.pl -n -c conf-lic.ini /usr/local/var/uls/weekly_l.zip
./import.pl -n -c conf-app.ini /usr/local/var/uls/weekly_a.zip
```

Note that if any sequence of newer dailies is available, they can be
appended after the weekly archive for a one-shot import.

## Importing dailies

When importing new dailies into an existing database, omit the -n argument
to avoid creating an empty dataset, meaning only daily changes are applied.

```
./import.pl -c conf-lic.ini /usr/local/var/uls/daily_l_{sun,mon}.zip
./import.pl -c conf-app.ini /usr/local/var/uls/daily_a_{sat,sun,mon}.zip
```

Typical ULS Publish Schedule
---

Zips are published 7-days a week, with full datasets weekly on weekends. The
daily files are not additive, and each new week requires importing the full
weekly in addition to the dailys (if up-to-the-date importing is desired.)

If everything is working smoothly at the FCC, weeklies are produced:

* Saturday mid-morning for Applications
* Sunday mid-morning for Licenses

Once a weekly is availble, it can be imported as a "new" (-n with import.pl)
database, re-creating the full data it conatins. To import dailies after,
the file named with the day the weekly was published should be used.

That is, one could import the app weekly then sat, sun, & mon dailies. For
license weeklies, the next-in-sequence is sun, not sat. Currently nothing
prevents the importer from blindly applying old dailies to an existing
database, so don't do that.

Also note that if today is Wednesday, you will not get "today's" changes
until Thursday. That is to say if you download or apply daily archives with
"wed" in the name, you're actually referring to changes from a week ago.
Same warning applies before Thurday's publication of that Wednesday data.

Check the `*.info` files for publication times, or to check when the tables
themselves were exported, extract the `counts` file:

```
7z x -so /usr/local/var/uls/daily_l_wed.zip counts
```

A Note on ULS Timestamps
---

The steps of downloading ULS table data and importing it are discrete.
Currently no logic exists to identify last-imported data, as timestamps in
the archive itself (and its FTP modification times) are not reliable.

Usually the dates of processing and archive modification on the FTP server
are the day after the data contained within (Monday data published on
Tuesday, etc.) However, ULS sometimes experiences failures and re-runs
processing at a later date.

Use the modification dates with a small grain of salt, especially if they
don't match the above expectation.

Todo
---

* Sanity check zips fed to import.pl, rejecting out-of-sequence imports

* Scan ULS zip dir to auto-import newer data automatically

* Identify date of data by table content, not created/modified timestamps

