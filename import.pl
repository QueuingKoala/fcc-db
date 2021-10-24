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
use JC::ULS::Data ();
use JC::ULS::Table ();

main(\@ARGV);
exit(0);

sub main {
	my $argv = shift; # \@ARGV
	my %Opts = (
		update => 0,	# is this an update run?
		analyze => 1,	# do we analyze after finish?
		remove => 0,	# may we remove existing DB file on create?
	);

	# Storage for table objects:
	my @tables;

	# Option handler for all table sources:
	my $opt_table = sub {
		my ($opt, $src) = (@_);
		my $name = uc($opt);

		# SQL statement data encapsulation object:
		my $data = JC::ULS::Data->new;
		my $meth = $data->can("query_$name")
			or die "No support for table type: $name";

		my $table = JC::ULS::Table->new;
		$table->define(
			name => $name,
			source => $src,
			update => $Opts{update},
		) or die "Table $name failed define: " . $table->error;

		# Set up the table object with processing statements:
		$data->$meth( $table );

		push @tables, $table;
	};

	Getopt::Long::GetOptionsFromArray( $argv,
		'schema|s=s' => \$Opts{schema},
		'indexes|i=s' => \$Opts{indexes},
		'db|database|d=s' => \$Opts{db},
		'update|u!' => \$Opts{update},
		'analyze|a!' => \$Opts{analyze},
		'remove|r!' => \$Opts{remove},
		'hd=s' => $opt_table,
		'am=s' => $opt_table,
		'en=s' => $opt_table,
		'ad=s' => $opt_table,
		'vc=s' => $opt_table,
		'hs=s' => $opt_table,
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
			remove => $Opts{remove},
		);
	}

	# Set up in-memory schema for ephemeral processing needs:
	runtime_schema( dbh=>$dbh );

	# Prepare all table queries:
	for my $table (@tables) {
		my $rc = $table->prepare(dbh=>$dbh);
		if (not $rc) {
			die sprintf("Table %s prepare failed: %s",
					$table->get('name'),
					$table->error
			);
		}
	}

	# Perform table imports:
	for my $table (@tables) {
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
	my $remove = $args{remove};

	# Need readable schema:
	die "Can't read schema: $!" if ( not -r $schema );

	# Read in index file, returned for later processing:
	open(my $fh_idx, '<', $indexes) or die "Index open failed: $!";
	my @index_sql = <$fh_idx>;
	close($fh_idx);

	if (-f $db) {
		if (not $remove) {
			die "Cannot remove existing DB without -r option";
		}
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

sub runtime_schema {
	my %args = (@_);
	my $dbh = $args{dbh} or die "No dbh";

	$dbh->do('CREATE TEMP TABLE t_vc_seen (sys_id INTEGER PRIMARY KEY)');
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

