DROP VIEW IF EXISTS tmp_ivc;

CREATE TEMP VIEW tmp_ivc AS
SELECT
	sys_id
	,callsign
	,license_status
	,grant_date
	,max(
		CASE license_status
			WHEN 'E' THEN
				expired_date
			ELSE
				--canceled_date
				coalesce( canceled_date, expired_date )
		END
	) AS end_date
	,last_action_date
FROM
	fcc.t_hd
WHERE
	length(callsign) = 4
	AND callsign NOT LIKE 'AL%'
	AND callsign NOT LIKE 'NL%'
	AND callsign NOT LIKE 'WL%'
	AND callsign NOT LIKE 'KP%'
	AND callsign NOT LIKE 'NP%'
	AND callsign NOT LIKE 'WP%'
	AND callsign NOT LIKE 'AH%'
	AND callsign NOT LIKE 'KH%'
	AND callsign NOT LIKE 'NH%'
	AND callsign NOT LIKE 'WH%'
GROUP BY
	callsign
;

