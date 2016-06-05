#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long ();
use DBI ();
use Data::Dumper qw(Dumper);
use lib 'lib';
use JC::ULS::Table ();

use constant {
	TDEF => { },	# table defs, created by populate_tdef()
	TABLES => [],	# table object storage
};

main(\@ARGV);
exit(0);

sub main {
	my $argv = shift; # \@ARGV
	my %Opts;

	# Populate TDEF constant hashref:
	populate_tdef();

	# Build source file options from TDEF keys:
	my $tdef = TDEF(); # constant %$TDEF
	my %tdef_opts;
	for (keys %$tdef ) {
		$tdef_opts{"${_}=s"} = \&opt_table;
	}

	Getopt::Long::GetOptionsFromArray( $argv,
		'schema|s=s' => \$Opts{schema},
		'indexes|i=s' => \$Opts{indexes},
		'db|database|d=s' => \$Opts{db},
		%tdef_opts,
	) or die "Options error";

	# Mandatory options:
	for (qw<db schema indexes>) {
		next if (defined $Opts{$_});
		die "Unspecified mandatory option: $_";
	}

	# Read in full indexes file, for use when we're done:
	open(my $fh, '<', $Opts{indexes}) or die "Index open failed: $!";
	my @index_sql = <$fh>;
	close($fh);

	# Create a new $dbh handle, w/ fresh schema:
	my $dbh = mk_db(db=>$Opts{db}, schema=>$Opts{schema});

	my $tables = TABLES(); # constant @$tables

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

sub opt_table {
	my ($opt, $src) = (@_);

	my $table = JC::ULS::Table->new;
	my $tdef = TDEF();	# constant %$tdef
	my $tables = TABLES();	# constant @$tables

	$table->define(
		name => uc($opt),
		source => $src,
		fields => $tdef->{$opt}{fields},
		date_fields => $tdef->{$opt}{date_fields} // [],
		query => $tdef->{$opt}{query},
	) or die "Table $opt failed: " . $table->error;

	push @$tables, $table;
}

sub populate_tdef {
	my $tdef = TDEF();

	$tdef->{hd} = {
		date_fields => [ 7..9, 42..43 ],
		fields => [ 1..2, 4..9, 42..43 ],
		query => qq[
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
		],
	};
	$tdef->{am} = {
		fields => [ 1..2, 4..9, 12..13, 15..17 ],
		query => qq[
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
		],
	};
	$tdef->{en} = {
		fields => [ 1..2, 4..11, 15..20, 22..23 ],
		query => qq[
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
		],
	};
}

