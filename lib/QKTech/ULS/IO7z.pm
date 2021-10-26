package QKTech::ULS::IO7z v0.1.0;

use strict;
use warnings;
use Carp ();

# --
# Constructors
# --

sub new {
	my $class = shift;
	$class = ref($class) || $class;
	my $self = {
		bin => undef,
		source => undef,
		fh => undef,
	};
	bless($self, $class);
}

# --
# Methods
# --

sub locate {
	my ($self, $bin) = (@_);
	$bin = `which 7z` if (not defined $bin);
	if (not defined $bin) {
		return $self->error("Failed to locate 7z binary");
	}
	chomp($bin);
	if (not -f $bin or not -x $bin) {
		return $self->error("7z binary missing or non-executable");
	}
	$self->{bin} = $bin;
	return 1;
}

sub setSource {
	my ($self, $path) = (@_);
	if (not -f $path) {
		return $self->error("file not present");
	}
	$self->{source} = $path;
	return 1;
}

sub list {
	my $self = shift;
	my @args = (qw[ l -bso0 -bsp0 ]);
	$self->invoke( \@args, @_ );
}

sub extract {
	my $self = shift;
	my @args = (qw[ x -so -bso0 -bsp0 ]);
	$self->invoke( \@args, @_ );
}

sub invoke {
	my $self = shift;
	my $argv = shift; # \@args
	my $src = $self->{source};
	if (not defined $src or not -r $src) {
		return $self->error("source unset or unreadable");
	}
	my @args = (@$argv, $src, @_);
	open(my $fh, '-|', $self->{bin}, @args)
		or return $self->error("7z extract failed: $!");
	if (not defined $fh) {
		return $self->error(sprintf("Open failed, code %d cause: %s", $!, "$!"));
	}
	$self->{fh} = $fh;
	return 1;
}

sub close {
	my $self = shift;
	return 0 if (not defined $self->{fh});
	my $err = "";
	close($self->{fh}) or $err = sprintf("exited rc=%d: %s", $? >>8 & 127, "$!");
	$self->{fh} = undef;
	return ($err) ? $self->error($err) : $? >>8 & 127;
}

sub readline {
	my $self = shift;
	my $fh = $self->{fh};
	if (not defined($fh)) {
		return $self->error("no extract operation is active");
	}
	if (my $line = defined($fh) ? <$fh> : undef) {
		chomp($line);
		$line =~ tr/\r$//d; # Strip MS-DOS style <CR> from line-end
		return $line;
	}
	return undef;
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
# End of class
# --

1;

