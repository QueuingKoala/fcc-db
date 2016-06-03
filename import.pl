#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long ();
use DBI ();
use Data::Dumper qw(Dumper);

main(\@ARGV);
exit(0);

sub main {
	my $argv = shift; # \@ARGV
	my %Opts;

	my @tables = qw(hd am en);

	Getopt::Long::GetOptionsFromArray( $argv,
		'schema|s=s' => \$Opts{schema},
		'indexes|i=s' => \$Opts{indexes},
		'db|database|d=s' => \$Opts{db},
		'hd=s' => \$Opts{hd},
		'am=s' => \$Opts{am},
		'en=s' => \$Opts{en},
	) or die "Options error";

	# Mandatory options:
	for (qw<db schema indexes>) {
		next if (defined $Opts{$_});
		die "Unspecified mandatory option: $_";
	}

	# Warn when skipping import tables:
	for (@tables) {
		next if (defined $Opts{$_});
		printf(STDERR "Omitting table %s\n", uc($_) );
	}

	# Read in full indexes file, for use when we're done:
	open(my $fh, '<', $Opts{indexes}) or die "Index open failed: $!";
	my @index_sql = <$fh>;
	close($fh);

	# Create a new $dbh handle, w/ fresh schema:
	my $dbh = mk_db(db=>$Opts{db}, schema=>$Opts{schema});

	my $imports = import_hr(dbh=>$dbh);

	# Perform table imports:
	for my $table (@tables) {
		# Skip tables for which there is no input:
		next if (not defined $Opts{$table});

		# Run the import:
		table_import(
			src_file => $Opts{$table},
			date_conv => $imports->{$table}{date_conv} // [],
			cols => $imports->{$table}{cols},
			dbh => $dbh,
			sth => $imports->{$table}{insert},
			table => $table,
		);
	}

	# Finish, setting DB tunables for normal use:
	finish_db( dbh=>$dbh, indexes=>\@index_sql );
}

sub mk_db {
	my %args = (@_);
	my $db = $args{db};
	my $schema = $args{schema};
	#my $Opts = shift; # \%Opts

	# Need readable schema:
	if ( not -r $schema ) {
		die "Can't read schema: $schema";
	}

	if (-f $db) {
		unlink($db) or die "remove old db failed: $!";
	}
	# New DB:
	my $dbh = DBI->connect("dbi:SQLite:db=$db",
		'', '', {
			AutoCommit => 1,
			PrintError => 0,
			RaiseError => 1,
			sqlite_allow_multiple_statements => 1,
		}
	) or die "DB connect failed: $DBI::errstr";

	# Enable FK enforcement:
	$dbh->do('PRAGMA foreign_keys(1);');
	my $fk_row = $dbh->selectrow_arrayref('PRAGMA foreign_keys')
		or die "FK row check failed to execute";
	if ($fk_row->[0] != 1) {
		die "FK is not enabled";
	}

	# MEMORY mode, for import:
	$dbh->do('PRAGMA journal_mode = MEMORY');
	$dbh->{AutoCommit} = 0;

	# Read in full schema:
	open(my $fh_schema, '<', $schema)
		or die "Schema open failed: $!";
	my @schema = <$fh_schema>;
	close($fh_schema);

	# Apply schema:
	$dbh->do( "@schema" );

	# Disable multiple-statements, no longer needed:
	$dbh->{sqlite_allow_multiple_statements} = 0;

	return $dbh;
}

sub finish_db {
	my %args = (@_);
	my $dbh = $args{dbh};
	my $indexes = $args{indexes}; # \@index_sql

	$dbh->{AutoCommit} = 1;

	# Build indexes:
	print(STDERR "Building indexes..");
	$dbh->{sqlite_allow_multiple_statements} = 1;
	$dbh->do( "@$indexes" );
	$dbh->{sqlite_allow_multiple_statements} = 0;
	print(STDERR "\n");

	# ANALYZE and reset journal_mode:
	print(STDERR "Analyze DB..");
	$dbh->do('ANALYZE');
	$dbh->do('PRAGMA journal_mode = DELETE');
	print(STDERR "\n");
}

sub table_import {
	my %args = (
		date_conv => [],
		@_
	);

	my $dbh = $args{dbh};			# dbh handle
	my $sth = $args{sth};			# insert SQL statement handle
	my $src_file = $args{src_file};		# source input file
	my $date_conv = $args{date_conv};	# array-ref of US-to-ISO date indexes
	my $cols = $args{cols};			# array-ref of insert column indexes
	my $table = $args{table};		# short name of table (for display)

	open(my $fh, '<', $src_file)
		or die "Open '$src_file' failed: $!";

	printf(STDERR "Importing table %s: ", uc($table) );

	# Process lines:
	while (my $line = <$fh>) {
		# Extract fields from record input:
		chomp $line;
		my @fields = split(/\|/, $line);

		# Set empty records to undef (SQL NULL):
		for (\(@fields)) {
			$$_ = undef if ( length($$_) == 0 );
		}

		# Normalize date:
		for (@$date_conv) {
			next if (not defined $fields[$_]);
			date_to_iso( \$fields[$_] );
		}

		# Extract columns of interest:
		my @row = @fields[@$cols];

		# Insert:
		eval {
			$sth->execute( (@row) );
		};
		if ($@) {
			print Dumper( \@row );
			$dbh->commit;
			die "$@";
		}
	}
	continue {
		if ($. % 100000 == 0) {
			$dbh->commit;
			printf(STDERR "%dk.. ", $. / 1000);
		}
	}

	printf(STDERR "%d.\n", $.);

	close($fh);
	$dbh->commit;
}

sub import_hr {
	my %args = (@_);
	my $dbh = $args{dbh};

	return {
		hd => {
			date_conv => [ 7..9, 42..43 ],
			cols => [ 1..2, 4..9, 42..43 ],
			insert => $dbh->prepare(qq[
				INSERT INTO t_hd (
					sys_id,
					uls_fileno,
					callsign,
					license_status,
					service_code,
					grant_date,
					expired_date,
					canceled_date,
					effective_date,
					last_action_date
				)
				VALUES (?,?,?,?,?,?,?,?,?,?)
			]),
		},
		am => {
			cols => [ 1..2, 4..9, 12..13, 15..17 ],
			insert => $dbh->prepare(qq[
				INSERT INTO t_am (
					sys_id,
					uls_fileno,
					callsign,
					op_class,
					group_code,
					region_code,
					trustee_callsign,
					trustee_indicator,
					sys_call_change,
					vanity_call_change,
					previous_callsign,
					previous_op_class,
					trustee_name
				)
				VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
			]),
		},
		en => {
			cols => [ 1..2, 4..11, 15..20, 22..23 ],
			insert => $dbh->prepare(qq[
				INSERT INTO t_en (
					sys_id,
					uls_fileno,
					callsign,
					entity_type,
					license_id,
					entity_name,
					first_name,
					mi,
					last_name,
					suffix,
					street,
					city,
					state,
					zip_code,
					po_box,
					attn,
					frn,
					type_code
				)
				VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			]),
		},
	};
}

sub date_to_iso {
	my $date = shift; # \$date_string, MM/DD/YYYY

	my @split = split(/\//, $$date);
	die "date_to_iso failure on: $$date" if (scalar(@split) != 3);
	$$date = sprintf('%s-%s-%s',
			$split[2],
			$split[0],
			$split[1],
	);
}

