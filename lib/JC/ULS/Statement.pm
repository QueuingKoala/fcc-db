package JC::ULS::Statement;

use strict;
use warnings;

# --
# Class-scoped vars
# --

# None.

# --
# Constructors
# --

# my $st = JC::ULS::Statement->(
#	fields => \@field_indexes,
#	sql => $sql_statement,
# );

sub new {
	my ($class, %args) = (@_);
	$class = ref($class) || $class;

	# Required args:
	for (qw[ fields sql ]) {
		return undef if (not exists $args{$_});
	}

	# Convert field numbers to array indexes (subtract 1):
	grep { --$_ } @{$args{fields}};

	my $self = { %args };
	bless $self, $class;
}

# --
# Methods
# --

# $rc = $st->prepare( dbh => $dbh );
#
# Return: Nothing. Dies on errors.

sub prepare {
	my ($self, %args) = (@_);

	my $dbh = $args{dbh}
		or die "No dbh passed";

	$self->{sth} = $dbh->prepare( $self->{sql} );
}

# $rc = $st->execute( row => \@input_row );
#
# Return: Nothing. Dies on errors.

sub execute {
	my ($self, %args) = (@_);

	# Required args:
	my $row = $args{row} # \@input_row
		or die "st->execute(): Missing row";
	my $sth = $self->{sth}
		or die "st->execute(): no sth. Forget prepare?";

	# Extract columns of interest:
	my $cols = $self->{fields}; # \@fields
	my @values = @$row[@$cols];

	# Insert:
	$sth->execute( @values );
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

1;
