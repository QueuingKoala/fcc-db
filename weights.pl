#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper qw(Dumper);

main( \@ARGV );
exit(0);

sub main {
	prep_weights();
	my $Values = VALUES();
	my $Weights = WEIGHTS();
	#print( Dumper( WEIGHTS() ) );

	while (my $call = <>) {
		chomp $call;

		my @letters = split(//, $call);
		my $weight = $Values->{inter_letter} * (length($call) - 1);
	
		for (@letters) {
			my $value = $Weights->{uc($_)} or die "No weight for $_";
			$weight += $value;
		}

		printf( "%s: %d\n",
				$call,
				$weight
		);
	}
}

sub prep_weights {
	# hashref constants:
	my $Values = VALUES();
	my $Weights = WEIGHTS();
	my $Symbols = SYMBOLS();

	for my $symb (keys %$Symbols) {
		my $code = $Symbols->{$symb};
		# Seed value with inter-symbols spaces:
		my $value = (length($code) - 1) * $Values->{inter_symbol};
		#printf( STDERR "DEBUG: Seeding symbol %s with weight %d\n",
		#		$symb,
		#		$value
		#);

		while ( my $element = substr($code, 0, 1, '') ) {
			#printf( STDERR "DEBUG: inspecting %s\n", $element );
			$value += eval {
				return $Values->{dash} if ($element eq '-');
				return $Values->{dot} if ($element eq '.');
				die "Bad element during prep: $element";
			};
			die "Unknown element: $element" if ($@);
			#printf( STDERR "DEBUG: value is now: %d\n", $value );
		}

		$Weights->{$symb} = $value;
	}
}

use constant {
	VALUES => {
		inter_symbol => 1,
		inter_letter => 3,
		dash => 3,
		dot => 1,
	},
	SYMBOLS => {
		A => '.-',
		B => '-...',
		C => '-.-.',
		D => '-..',
		E => '.',
		F => '..-.',
		G => '--.',
		H => '....',
		I => '..',
		J => '.---',
		K => '-.-',
		L => '.-..',
		M => '--',
		N => '-.',
		O => '---',
		P => '.--.',
		Q => '--.-',
		R => '.-.',
		S => '...',
		T => '-',
		U => '..-',
		V => '...-',
		W => '.--',
		X => '-..-',
		Y => '-.--',
		Z => '--..',
		0 => '-----',
		1 => '.----',
		2 => '..---',
		3 => '...--',
		4 => '....-',
		5 => '.....',
		6 => '-....',
		7 => '--...',
		8 => '---..',
		9 => '----.',
	},
	WEIGHTS => { },
};

