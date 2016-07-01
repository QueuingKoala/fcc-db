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

sub query_EN {
	my ($self, $table) = (@_);

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

#	if ( $table->get('update') ) {
#		$table->addQuery(
#			fields => [ 2 ],
#			sql => qq[
#				DELETE FROM t_vc
#				WHERE
#				sys_id = ?
#			],
#		) or die "VC sth (delete) failed: " . $table->error;
#	}

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

1;
