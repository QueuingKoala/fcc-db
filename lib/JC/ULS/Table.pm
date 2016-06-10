package JC::ULS::Table;

use strict;
use warnings;
use Carp ();
use Data::Dumper qw(Dumper);
use JC::ULS::Statement ();

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
	};
	bless $self, $class;
}

# --
# Methods
# --

# $rcBool = $t->define(
#	name => $display_name,		# Required, display name of table
#	source => $file,		# Required, source file with records
#	update => $bool,		# Required, indicates if we're updating
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
	for (qw[ name update source ]) {
		return $self->error("missing arg: $_") if (not exists $args{$_});
	}

	$self->{args} = \%args;
	return 1;
}

# $rcBool = $t->addQuery(
#	fields => \@field_cols,		# (1-indexed)
#	sql => $sql_statement,
# );
#
# Updates $t->error on failure with cause.

sub addQuery {
	my ($self, %args) = (@_);

	for (qw[ fields sql ]) {
		return $self->error("missing arg: $_") if (not exists $args{$_});
	}

	my $st = JC::ULS::Statement->new( %args )
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

	# Convert field numbers to array indexes (subtract 1):
	grep { --$_ } @$dates;

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
	my $dbh = $args{dbh};			# dbh handle

	# Short names of object attribute:
	my $update = $self->get('update');	# bool, if we're updating DB.
	my $statements = $self->{statements};	# \@statement_objects
	my $src_file = $self->get('source');	# source input file
	my $date_conv = $self->get('date_fields'); # array-ref of US date indexes
	my $name = $self->get('name');		# short name of table (for display)

	print(STDERR "Importing table $name: " );

	eval {
		open(my $fh, '<', $src_file) or die "Open '$src_file' failed: $!";
		while (my $line = <$fh>) {
			# Extract fields from record input:
			chomp $line;
			my @fields = split(/\|/, $line);

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
			if ($. % 100000 == 0) {
				$dbh->commit if (not $update);
				printf(STDERR "%dk.. ", $. / 1000);
			}
		}

		printf(STDERR "%d.\n", $.);

		close($fh);
		$dbh->commit if (not $update);
	};
	if ($@) {
		my $err = "$@";
		eval { $dbh->rollback; };
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
