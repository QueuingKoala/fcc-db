package JC::ULS::Data;

use strict;
use warnings;
#use Carp ();

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
	bless { }, $class;
}

# --
# Methods
# --

sub query_HD {
	my ($self, $table) = (@_);

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

sub query_AM {
	my ($self, $table) = (@_);

	my $exec_district = sub {
		my ($row, $out) = (@_);
		my $district = eval {
			for my $call ( $row->[5-1] // "" ) {
				return 11 if ( $call =~ /^(AL|KL|NL|WL)/ );
				return 12 if ( $call =~ /^(KP|NP|WP)/ );
				return 13 if ( $call =~ /^(AH|KH|NH|WH)/ );
				return undef if ( $call =~ /^[AKNW]F[0-9]EMA$/ );
				return undef if ( length($call) == 3 );
				if ( $call =~ /^[[:alpha:]]+([0-9])[[:alpha:]]+$/ ) {
					return 10 if ($1 eq 0);
					return $1;
				}
				return undef;
			}
		};
		push( @$out, $district );
	};

	$table->addQuery(
		fields => [ 2..3, 5..10, 13..14, 16..18 ],
		callbacks => [ $exec_district ],
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
				trustee_name,
				district
			)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		],
	) or die "AM sth failed: " . $table->error;
}

sub query_EN {
	my ($self, $table) = (@_);

	my $exec_district = sub {
		my ($row, $out) = (@_);
		my $district = eval {
			for my $state ( $row->[18-1] // "" ) {
				return 1 if ( $state =~ /^(me|vt|nh|ma|ri|ct)$/i );
				return 2 if ( $state =~ /^(ny|nj)$/i );
				return 3 if ( $state =~ /^(pa|de|md|dc)$/i );
				return 4 if ( $state =~ /^(ky|va|tn|nc|sc|ga|al|fl)$/i );
				return 5 if ( $state =~ /^(nm|tx|ok|ar|la|ms)$/i );
				return 6 if ( $state =~ /^ca$/i );
				return 7 if ( $state =~ /^(wa|or|id|mt|wy|nv|ut|az)$/i );
				return 8 if ( $state =~ /^(mi|oh|wv)$/i );
				return 9 if ( $state =~ /^(wi|il|in)$/i );
				return 10 if ( $state =~ /^(co|nd|sd|ne|ks|mn|ia|mo)$/i );
				return 11 if ( $state =~ /^ak$/i );
				return 12 if ( $state =~ /^(pr|vi)$/i );
				return 13 if ( $state =~ /^(hi|as|gu|mp)$/i );
				return undef;
			}
		};
		push( @$out, $district);
	};

	$table->addQuery(
		fields => [ 2..3, 5..12, 16..21, 23..24 ],
		callbacks => [ $exec_district ],
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
				type_code,
				district
			)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		],
	) or die "EN sth failed: " . $table->error;
}

sub query_AD {
	my ($self, $table) = (@_);

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

sub query_VC {
	my ($self, $table) = (@_);

	# When updating, initial duplicate sys_id rows must first be removed.
	# These are part of the composit PK which may not always be replaced.

	if ( $table->get('update') ) {
		$table->addQuery(
			fields => [ 2, 2 ],
			sql => qq[
				DELETE FROM t_vc
				WHERE
				sys_id = ?
				AND
				( (SELECT count(sys_id) FROM temp.t_vc_seen WHERE sys_id = ?) = 0
				)
			],
		) or die "VC sth (delete) failed: " . $table->error;

		$table->addQuery(
			fields => [ 2 ],
			sql => qq[
				INSERT OR IGNORE INTO temp.t_vc_seen (
					sys_id
				)
				VALUES (?)
			],
		) or die "VC sth (temp sys_id) failed: " . $table->error;
	}

	$table->addQuery(
		fields=> [ 2..3, 5..6 ],
		sql => qq[
			INSERT INTO t_vc (
				sys_id,
				uls_fileno,
				pref_order,
				callsign
			)
			VALUES (?,?,?,?)
		],
	) or die "VC sth failed: " . $table->error;
}

sub query_HS {
	my ($self, $table) = (@_);

	$table->dateFields(
		dates => [ 5 ],
	) or die "HS dates failed: " . $table->error;

	$table->addQuery(
		fields => [ 2, 4..6 ],
		sql => qq[
			INSERT INTO t_hs (
				sys_id,
				callsign,
				log_date,
				code
			)
			VALUES (?,?,?,?)
		],
	) or die "HS sth failed: " . $table->error;
}

1;
