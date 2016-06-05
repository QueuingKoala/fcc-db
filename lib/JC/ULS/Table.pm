package JC::ULS::Table;

use strict;
use warnings;
use Data::Dumper qw(Dumper);

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
	bless {}, $class;
}

# --
# Methods
# --

# $rcBool = $t->define(
#	name => $display_name,		# Required, display name of table
#	source => $file,		# Required, source file with records
#	fields => \@field_nums,		# Required, fields of records for query values
#	date_fields => \@field_nums,	# Optional, fields for US->ISO conversion
#	query => $sql,			# Required, SQL to insert a record
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
	for (qw[ name source fields query ]) {
		return $self->error("missing arg: $_") if (not exists $args{$_});
	}

	$self->{args} = \%args;
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

# $rcBool = $t->prepare(
#	dbh => $dbh,			# Required, dbh object
# );
#
# On success, stores sth for future access with $t->get('sth').
# Updates $t->error on failure with cause.

sub prepare {
	my ($self, %args) = (@_);

	# Must have dbh:
	return $self->error("no dbh passed") if (not exists $args{dbh});
	my $dbh = $args{dbh};

	eval {
		$self->{args}{sth} = $dbh->prepare( $self->get('query') );
	};
	if ($@) {
		return $self->error("$@");
	}

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
	my $sth = $self->get('sth');		# insert SQL statement handle
	my $src_file = $self->get('source');	# source input file
	my $cols = $self->get('fields');	# array-ref of insert column indexes
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
	};
	if ($@) {
		return $self->error("Import error: $@");
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
