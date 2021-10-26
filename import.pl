#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long ();
use DBI ();
#use Data::Dumper qw(Dumper);

BEGIN {
	require File::Spec;
	my ($vol, $dir) = File::Spec->splitpath($0);
	unshift @INC, File::Spec->catdir($dir, 'lib');
};
use QKTech::ULS::IO7z ();
use QKTech::ULS::Data ();
use QKTech::ULS::Table ();

use constant {
	MONTH_TO_DEC => {
		JAN => '01',	FEB => '02',	MAR => '03',
		APR => '04',	MAY => '05',	JUN => '06',
		JUL => '07',	AUG => '08',	SEP => '09',
		OCT => '10',	NOV => '11',	DEC => '12',
	},
	TZ_TO_OFF => {
		EDT => '-04:00',
		EST => '-05:00',
	},
};

main(\@ARGV);
exit(0);

sub usage_exit {
	print <<_EOF;
ULS Amateur Radio DB Import Tool

SYNOPSIS

$0
-d DB_FILE [-n -s SCHEMA -i INDEXES]
[-c CONFIG_INI]

Options can be moved to config using long-names as keys (no dashes) under
an INI section called [config].

LONG-OPTION NAMES

-d: --db
-n: --new     (in INI file, set to 1 for true; 0/omitted for false)
-s: --schema
-i: --indexes
_EOF
	exit 0;
}

sub parse_options {
	my $argv = shift; # \@ARGV
	my $Opts = {
		new => 0,	# create fresh DB, overwriting any old one?
	};

	my $ini_opts = sub {
		my ($opt, $conf) = (@_);
		(-r $conf) or fatal("Config missing or not readable");
		require Config::IniFiles;
		local @Config::IniFiles::errors; # surpress 'only used once' warning
		my $cfg = Config::IniFiles->new( -file => $conf )
			or fatal("INI parse error(s):", @Config::IniFiles::errors);
		for (qw[ new database schema indexes ]) {
			my $val = $cfg->val('config', $_);
			$Opts->{$_} = $val if (defined $val);
		}
	};

	Getopt::Long::GetOptionsFromArray( $argv,
		'help|h|?'	=> \&usage_exit,
		'config|c=s'	=> $ini_opts,
		'new|n!'	=> \$Opts->{new},
		'db|database|d=s' => \$Opts->{db},
		'schema|s=s'	=> \$Opts->{schema},
		'indexes|i=s'	=> \$Opts->{indexes},
	) or die( "Options error" );

	if ($Opts->{new}) {
		(defined $Opts->{schema}) or fatal("Missing --schema file");
		(defined $Opts->{indexes}) or fatal("Missing --indexes file");
		if (-f $Opts->{db}) {
			unlink($Opts->{db}) or fatal("Removing existing db failed: $!");
		}
	}

	return $Opts;
}

sub fatal {
	printf(STDERR "%s\n", $_) for (@_);
	exit 1;
}

sub main {
	my $argv = shift; # \@ARGV
	my $Opts = parse_options( $argv );

	my $io7z = QKTech::ULS::IO7z->new();
	$io7z->locate() or fatal("7z locate failed");

	# Sanity-check remaining arguments, vetting as compresed archives.

	check_archives( $argv, $io7z );

	# Connect DB, creating or opening as required.

	my $dbh;
	my $index_sql = undef;	# holds reference to full index SQL text
	if ($Opts->{new}) {
		($dbh, $index_sql) = mk_db(
			db => $Opts->{db},
			schema => $Opts->{schema},
			indexes => $Opts->{indexes},
		);
	}
	else {
		$dbh = open_db( db => $Opts->{db} );
	}

	# Loop over remaining arguments, processing archives.

	for my $arch (@$argv) {
		runtime_schema( dbh => $dbh ); # (re)set ephemeral processing

		my $tables = archive_prep(
			new => $Opts->{new},
			arch => $arch,
			dbh => $dbh,
			io7z => $io7z, # will set the archive path in-object
		);

		table_import(
			tables => $tables,
			dbh => $dbh,
			io7z => $io7z, # uses existing archive path
		);

		$dbh->commit() if (not $Opts->{new});
	}
	continue {
		$io7z->close();

		if ($Opts->{new}) { # new weekly post-import needs
			print( STDERR "Building indexes.." );
			index_db( $dbh, $index_sql );
			print( STDERR " Analyze DB.." );
			$dbh->do('ANALYZE');
			$dbh->commit();
			$dbh->do('PRAGMA journal_mode = DELETE');
			print( STDERR " Done.\n" );
			$Opts->{new} = 0;
		}
	}

	# Finish, setting DB tunables for normal use.

	$dbh->{AutoCommit} = 1;
	$dbh->do('ANALYZE');
}

sub check_archives {
	my ($argv, $io7z) = (@_);

	# Ensure each archive lists contents successfully.

	for my $arch (@$argv) {
		$io7z->setSource( $arch )
			or fatal("archive access failure: " . $io7z->error());
		$io7z->list()
			or fatal("archive check failure: " . $io7z->error());
	}

	$io7z->close();
}

sub archive_prep {
	my (%opts) = (@_);
	my $arch = $opts{arch};
	my $dbh = $opts{dbh};
	my $io7z = $opts{io7z};
	my $new = $opts{new};

	$io7z->setSource( $arch )
		or fatal("7z source-set fail: " . $io7z->error());
	$io7z->extract('counts')
		or fatal("7z extract fail: " . $io7z->error());

	my @tables;
	my $iso8601 = undef;
	LINE:
	while( defined(my $line = $io7z->readline()) ) {
	  for ($line) {
		# Convert create-date line to ISO-8601

		if ( m/^File Creation Date:
			\s+\w+			# weekday
			\s+(\w+)		# month ($1)
			\s+(\d+)		# day ($2)
			\s+(\d{2}:\d{2}:\d{2})	# HH:MM:SS ($3)
			\s+(\w+)		# timezone ($4)
			\s+(\d{4})		# year ($5)
			$/x
		) {
			# Convert Mo/TZ to usable ISO-8601 terms.
			my $mo = MONTH_TO_DEC()->{uc($1)} // 13;
			my $tz = TZ_TO_OFF()->{uc($4)} // '-04:00';
			$iso8601 = "$5-$mo-$2T$3$tz";
		}

		# Prep each table file of interest for query-data

		elsif ( m/(([A-Z]{2})\.dat)$/
		) {
			my $ulsData = QKTech::ULS::Data->new();
			my $prep = $ulsData->can("query_$2") or next LINE;

			my $table = QKTech::ULS::Table->new();
			$table->define(
				name => $2,
				source => "$1",
				iso8601 => $iso8601,
				new => $new,
			) or fatal("Table $2 failed defined: " . $table->error());

			$ulsData->$prep( $table ); # prep table via Data's query_* method
			push(@tables, $table);
		}
	  }
	}

	# TODO: future date safety checks applying dalies

	return \@tables;
}

sub table_import {
	my (%opts) = (@_);
	my $dbh = $opts{dbh};
	my $io7z = $opts{io7z};
	my $tables = $opts{tables};

	# Prepare all table queries

	for my $table (@$tables) {
		my $rc = $table->prepare( dbh => $dbh );
		if (not $rc) {
			fatal( sprintf("Table %s prepare failed: %s",
					$table->get('name'),
					$table->error
			) );
		}
	}

	# Perform table imports

	for my $table (@$tables) {
		my $rc = $table->import(
			dbh => $dbh,
			io7z => $io7z,
		);
		if (not $rc) {
			fatal( sprintf("Table %s import failed: %s",
					$table->get('name'),
					$table->error
			) );
		}
	}
}

sub mk_db {
	my %args = (@_);
	my $db = $args{db};
	my $schema = $args{schema};
	my $indexes = $args{indexes};

	# Need readable schema:
	fatal("Can't read schema: $!") if ( not -r $schema );

	# Read in index file, returned for later processing:
	open(my $fh_idx, '<', $indexes) or fatal("Index open failed: $!");
	my @index_sql = <$fh_idx>;
	close($fh_idx);

	# Read in full schema:
	open(my $fh_schema, '<', $schema)
		or fatal("Schema open failed: $!");
	my @schema = <$fh_schema>;
	close($fh_schema);

	if (-f $db) {
		unlink($db) or fatal("Removing old db failed: $!");
	}
	# New DB:
	my $dbh = DBI->connect("dbi:SQLite:db=$db",
		'', '', {
			AutoCommit => 1,
			PrintError => 0,
			RaiseError => 1,
			sqlite_use_immediate_transaction => 0,
			sqlite_allow_multiple_statements => 1,
		}
	) or fatal("DB connect failed: $DBI::errstr");

	# Enable FK enforcement:
	db_enable_fk( $dbh );

	# MEMORY mode, for import:
	$dbh->do('PRAGMA journal_mode = MEMORY');
	$dbh->{AutoCommit} = 0;

	# Apply schema:
	$dbh->do( "@schema" );

	# Disable multiple-statements, no longer needed:
	$dbh->{sqlite_allow_multiple_statements} = 0;

	return ($dbh, \@index_sql);
}

sub open_db {
	my %args = (@_);
	my $db = $args{db};

	# Need readable DB:
	if ( not -r $db ) {
		fatal("Database not readable: $db");
	}

	# open DB:
	my $dbh = DBI->connect("dbi:SQLite:db=$db",
		'', '', {
			AutoCommit => 1,
			PrintError => 0,
			RaiseError => 1,
			sqlite_use_immediate_transaction => 0,
		}
	) or fatal("DB connect failed: $DBI::errstr");

	# Enable FK enforcement:
	db_enable_fk( $dbh );

	$dbh->{AutoCommit} = 0;

	return $dbh;
}

sub db_enable_fk {
	my $dbh = shift;

	$dbh->do('PRAGMA foreign_keys(1);');
	my $fk_row = $dbh->selectrow_arrayref('PRAGMA foreign_keys')
		or fatal("FK row check failed to execute");
	if ($fk_row->[0] != 1) {
		fatal("FK is not enabled");
	}
}

sub runtime_schema {
	my %args = (@_);
	my $dbh = $args{dbh} or fatal("No dbh to prep runtime");

	$dbh->do('DROP TABLE IF EXISTS temp.t_vc_seen');
	$dbh->do('CREATE TEMP TABLE t_vc_seen (sys_id INTEGER PRIMARY KEY)');
}

sub index_db {
	my ($dbh, $indexes) = (@_);

	$dbh->{AutoCommit} = 1;
	$dbh->{sqlite_allow_multiple_statements} = 1;
	$dbh->do( "@$indexes" );
	$dbh->{sqlite_allow_multiple_statements} = 0;
	$dbh->{AutoCommit} = 0;
}

