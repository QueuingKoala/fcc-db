package QKTech::ULS::Table;

use strict;
use warnings;
use Carp ();
#use Data::Dumper qw(Dumper);
use QKTech::ULS::Statement ();

# --
# Class-scoped vars
# --

# None.

# --
# Constructors
# --

sub new {
	my $class = shift;
	$class = ref($class) || $class;
	my $self = {
		statements => [],
		callbacks => [],
	};
	bless $self, $class;
}

# --
# Methods
# --

# $rcBool = $t->define(
#	name => $display_name,		# Required, display name of table
#	source => $file,		# Required, source file with records
#	new => $bool,			# Required, indicates initial DB load
# );
#
# Updates $t->error on failure with cause.

sub define {
	my $self = shift;
	my %args = (
		date_fields => [],
		@_
	);

	# Verify required args:
	for (qw[ name new source ]) {
		return $self->error("missing arg: $_") if (not exists $args{$_});
	}

	$self->{args} = \%args;
	return 1;
}

# $rcBool = $t->addQuery(
#	fields => \@field_cols,		# (1-indexed)
#	sql => $sql_statement,
#	[ callbacks => \@callback_coderefs, ]
# );
#
# Updates $t->error on failure with cause.

sub addQuery {
	my ($self, %args) = (@_);

	for (qw[ fields sql ]) {
		return $self->error("missing arg: $_") if (not exists $args{$_});
	}

	my $st = QKTech::ULS::Statement->new( %args )
		or return $self->error("statement object failed create");

	my $statements = $self->{statements}; # \@statements
	push @$statements, $st;

	return 1;
}

# $value = $t->get( $attr );
#
# Updates $t->error if no such attribute.

sub get {
	my ($self, $key) = (@_);

	if (not exists $self->{args}{$key}) {
		return $self->error("no such attribute: $key");
	}

	return $self->{args}{$key};
}

# $rcBool = $t->dateFields(
#	dates => \@date_fields,
# );
#
# Updates $t->error on failure with cause.

sub dateFields {
	my ($self, %args) = (@_);

	my $dates = $args{dates} # \@date_fields
		or return $self->error("Missing required arg: 'dates'");

	# Convert field numbers to array indexes (subtract 1, except negatives):
	grep { --$_ if ($_ > 0) } @$dates;

	$self->{args}{date_fields} = $dates;
	return 1;
}

# $rcBool = $t->prepare(
#	dbh => $dbh,			# Required, dbh object
# );
#
# On success, prepares all statement objects in the table.
#
# On failure, Updates $t->error with cause.

sub prepare {
	my ($self, %args) = (@_);

	my $dbh = $args{dbh} or
		return $self->error("no dbh passed");
	my $statements = $self->{statements}; # \@statements

	eval {
		$_->prepare(dbh=>$dbh) for (@$statements);
	};
	return $self->error("$@") if ($@);

	return 1;
}

# $rcBool = $t->import(
#	dbh => $dbh,			# Required, dbh object
# );
#
# Updates $t->error on failure with cause.

sub import {
	my ($self, %args) = (@_);
	my $dbh = $args{dbh};	# dbh handle
	my $io7z = $args{io7z};	# IO7z, the archive interface object

	# Short names of object attribute:
	my $new = $self->get('new');		# bool, if new DB
	my $statements = $self->{statements};	# \@statement_objects
	my $callbacks = $self->{callbacks};	# \@callback_subrefs
	my $src_file = $self->get('source');	# source input filename
	my $date_conv = $self->get('date_fields'); # array-ref of US date indexes
	my $name = $self->get('name');		# short name of table (for display)

	print(STDERR "Importing table $name: " );

	eval {
		$io7z->extract( $src_file )
			or die("Extract '$src_file' failed: ". $io7z->error());
		my $nr = 0;
		while ( defined(my $line = $io7z->readline()) ) {
			++$nr;
			# Extract fields from record input:
			my @fields = split(/\|/, $line, -1);

			# Normalize record fields:
			#  * strip leading/trailing whitespace
			#  * set empty records to undef (SQL NULL)
			for (\(@fields)) {
				$$_ =~ s/^\s+//;
				$$_ =~ s/\s+$//;
				$$_ = undef if ( length($$_) == 0 );
			}

			# Normalize date:
			for (@$date_conv) {
				next if (not defined $fields[$_]);
				date_to_iso( \$fields[$_] );
			}

			# Execute all statements:
			for my $st (@$statements) {
				$st->execute( row => \@fields );
			}
		}
		continue {
			# Commit every 100k:
			if ($new and $nr % 100000 == 0) {
				$dbh->commit();
			}
			# Display progress every 250k:
			if ($nr % 250000 == 0) {
				printf(STDERR "%dk.. ", $nr / 1000);
			}
		}

		printf(STDERR "%d.\n", $nr);

		if ( not defined(my $rc = $io7z->close()) ) {
			die("Archive extract terminated: " . $io7z->error());
		}
		$dbh->commit() if ($new);
	};
	if ($@) {
		my $err = "$@";
		eval { $dbh->rollback(); };
		Carp::confess( "Import error: $err" );
	}

	return 1;
}

sub error {
	my $self = shift;
	my $err = shift;

	# Accessor:
	return $self->{error} // '' if (not defined $err);
	# Setter:
	$self->{error} = $err;
	return wantarray ? (@_) : shift // undef;
}

# --
# Procedural helpers
# --

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

1;
