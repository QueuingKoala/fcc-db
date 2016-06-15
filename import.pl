#!/usr/bin/perl

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
use JC::ULS::Table ();

use constant {
	TABLES => [],	# table object storage
};

main(\@ARGV);
exit(0);

sub main {
	my $argv = shift; # \@ARGV
	my %Opts = (
		update => 0,	# is this an update run?
		analyze => 1,	# do we analyze after finish?
	);

	# Refs to hashref constants:
	my $tables = TABLES(); # constant @$tables

	# Option handler for all table sources:
	my $opt_table = sub {
		my ($opt, $src) = (@_);

		my $table = mk_table(
			name => uc($opt),
			source => $src,
			update => $Opts{update},
		) or die "mk_table failed on $opt";

		push @$tables, $table;
	};

	Getopt::Long::GetOptionsFromArray( $argv,
		'schema|s=s' => \$Opts{schema},
		'indexes|i=s' => \$Opts{indexes},
		'db|database|d=s' => \$Opts{db},
		'update|u!' => \$Opts{update},
		'analyze|a!' => \$Opts{analyze},
		'hd=s' => $opt_table,
		'am=s' => $opt_table,
		'en=s' => $opt_table,
		'ad=s' => $opt_table,
		'vc=s' => $opt_table,
	) or die "Options error";

	# Mandatory options:
	my @need_opts = ('db');
	if (not $Opts{update}) {
		push @need_opts, qw(schema indexes);
	}
	for (@need_opts) {
		next if (defined $Opts{$_});
		die "Unspecified mandatory option: $_";
	}

	# Create or open the DB, as required:
	my $dbh;
	my $index_sql = undef;

	if ($Opts{update}) {	# open existing db:
		$dbh = open_db( db=>$Opts{db} );
	}
	else {			# create new db:
		# create db, w/ fresh schema; save indexes:
		($dbh, $index_sql) = mk_db(
			db => $Opts{db},
			schema => $Opts{schema},
			indexes => $Opts{indexes},
		);
	}

	# Prepare all table queries:
	for my $table (@$tables) {
		my $rc = $table->prepare(dbh=>$dbh);
		if (not $rc) {
			die sprintf("Table %s prepare failed: %s",
					$table->get('name'),
					$table->error
			);
		}
	}

	# Perform table imports:
	for my $table (@$tables) {
		my $rc = $table->import(dbh=>$dbh);
		if (not $rc) {
			die sprintf("Table %s import failed: %s",
					$table->get('name'),
					$table->error
			);
		}
	}

	# Commit when updating, as each table didn't:
	if ($Opts{update}) {
		print( STDERR "Commit changes.." );
		$dbh->commit;
		print( STDERR "\n" );
	}

	# Finish, setting DB tunables for normal use:
	finish_db(
		dbh => $dbh,
		indexes => $index_sql,
		update => $Opts{update},
		analyze => $Opts{analyze},
	);
}

sub mk_db {
	my %args = (@_);
	my $db = $args{db};
	my $schema = $args{schema};
	my $indexes = $args{indexes};

	# Need readable schema:
	die "Can't read schema: $!" if ( not -r $schema );

	# Read in index file, returned for later processing:
	open(my $fh_idx, '<', $indexes) or die "Index open failed: $!";
	my @index_sql = <$fh_idx>;
	close($fh_idx);

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
	db_enable_fk( $dbh );

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

	return ($dbh, \@index_sql);
}

sub open_db {
	my %args = (@_);
	my $db = $args{db};

	# Need readable DB:
	if ( not -r $db ) {
		die "Database not readable: $db";
	}

	# open DB:
	my $dbh = DBI->connect("dbi:SQLite:db=$db",
		'', '', {
			AutoCommit => 1,
			PrintError => 0,
			RaiseError => 1,
		}
	) or die "DB connect failed: $DBI::errstr";

	# Enable FK enforcement:
	db_enable_fk( $dbh );

	$dbh->{AutoCommit} = 0;

	return $dbh;
}

sub db_enable_fk {
	my $dbh = shift;

	$dbh->do('PRAGMA foreign_keys(1);');
	my $fk_row = $dbh->selectrow_arrayref('PRAGMA foreign_keys')
		or die "FK row check failed to execute";
	if ($fk_row->[0] != 1) {
		die "FK is not enabled";
	}
}

sub finish_db {
	my %args = (@_);
	my $dbh = $args{dbh};
	my $indexes = $args{indexes};	# \@index_sql
	my $update = $args{update};	# bool, if updating
	my $analyze = $args{analyze};	# bool, if analyzing

	$dbh->{AutoCommit} = 1;

	# Build indexes, for new DBs only
	if (not $update) {
		print(STDERR "Building indexes..");
		$dbh->{sqlite_allow_multiple_statements} = 1;
		$dbh->do( "@$indexes" );
		$dbh->{sqlite_allow_multiple_statements} = 0;
		print(STDERR "\n");
	}

	# ANALYZE and reset journal_mode:
	if ($analyze) {
		print(STDERR "Analyze DB..");
		$dbh->do('ANALYZE');
	}
	else {
		print(STDERR "(Skipping Analyze)");
	}
	$dbh->do('PRAGMA journal_mode = DELETE');
	print(STDERR "\n");
}

# $table = mk_table(
#	name => $disp_name,
#	update => $bool,
#	source => $file,
# ) or die "failure";

sub mk_table {
	my %args = (@_);
	for (qw[name update source]) {
		die "mk_table(): missing arg $_" if (not exists $args{$_});
	}

	my $table = JC::ULS::Table->new;
	$table->define(
		%args
	) or die "Table $args{name} failed define: " . $table->error;

	for ($args{name}) {
		query_hd($table) if ($_ eq "HD");
		query_am($table) if ($_ eq "AM");
		query_en($table) if ($_ eq "EN");
		query_ad($table) if ($_ eq "AD");
		query_vc($table) if ($_ eq "VC");
	}

	return $table;
}

sub query_hd {
	my $table = shift;

	$table->dateFields(
		dates => [ 8..10, 43..44 ],
	) or die "HD dates failed: " . $table->error;

	$table->addQuery(
		fields => [ 2..3, 5..10, 43..44 ],
		sql => qq[
			INSERT OR REPLACE INTO t_hd (
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
		],
	) or die "HD sth failed: " . $table->error;
}

sub query_am {
	my $table = shift;

	$table->addQuery(
		fields => [ 2..3, 5..10, 13..14, 16..18 ],
		sql => qq[
			INSERT OR REPLACE INTO t_am (
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
		],
	) or die "AM sth failed: " . $table->error;
}

sub query_en {
	my $table = shift;

	$table->addQuery(
		fields => [ 2..3, 5..12, 16..21, 23..24 ],
		sql => qq[
			INSERT OR REPLACE INTO t_en (
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
		],
	) or die "EN sth failed: " . $table->error;
}

sub query_ad {
	my $table = shift;

	$table->dateFields(
		dates => [ 11, 22 ],
	) or die "HD dates failed: " . $table->error;

	$table->addQuery(
		fields => [ 2..3, 5..6, 11, 16..17, 19, 22 ],
		sql => qq[
			INSERT OR REPLACE INTO t_ad (
				sys_id,
				uls_fileno,
				purpose,
				status,
				receipt_date,
				orig_purpose,
				waver_req,
				has_attachment,
				entry_date
			)
			VALUES (?,?,?,?,?,?,?,?,?)
		],
	) or die "AD sth failed: " . $table->error;
}

sub query_vc {
	my $table = shift;

	if ( $table->get('update') ) {
		$table->addQuery(
			fields => [ 2 ],
			sql => qq[
				DELETE FROM t_vc
				WHERE
				sys_id = ?
			],
		) or die "VC sth (delete) failed: " . $table->error;
	}

	$table->addQuery(
		fields=> [ 2..3, 5..6 ],
		sql => qq[
			INSERT OR REPLACE INTO t_vc (
				sys_id,
				uls_fileno,
				pref_order,
				callsign
			)
			VALUES (?,?,?,?)
		],
	) or die "VC sth failed: " . $table->error;
}

