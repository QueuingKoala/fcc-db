DROP VIEW IF EXISTS tmp_ivc;

CREATE TEMP VIEW tmp_ivc AS
SELECT
	hd.sys_id
	,hd.callsign
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
	,am.district
FROM
	fcc.t_hd hd
	JOIN
	fcc.t_am am
		ON (hd.sys_id = am.sys_id)
WHERE
	length(hd.callsign) = 4
	AND
	am.district >= 1
	AND
	am.district <= 10
GROUP BY
	hd.callsign
;

