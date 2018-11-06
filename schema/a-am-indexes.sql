--- FCC App DB Indexes

-- t_hd
CREATE INDEX idx_hd_ulsfileno ON t_hd (uls_fileno)
	WHERE uls_fileno IS NOT NULL;
CREATE INDEX idx_hd_callsign ON t_hd (callsign);
CREATE INDEX idx_hd_licstatus ON t_hd (license_status);

-- t_ad
CREATE INDEX idx_ad_ulsfileno ON t_ad (uls_fileno)
	WHERE uls_fileno IS NOT NULL;

-- t_am
CREATE INDEX idx_am_ulsfileno ON t_am (uls_fileno)
	WHERE uls_fileno IS NOT NULL;
CREATE INDEX idx_am_callsign ON t_am (callsign);

-- t_vc
CREATE INDEX idx_vc_callsign ON t_vc (callsign);

-- t_en
CREATE INDEX idx_en_ulsfileno ON t_en (uls_fileno)
	WHERE uls_fileno IS NOT NULL;
CREATE INDEX idx_en_callsign ON t_en (callsign);
CREATE INDEX idx_en_state ON t_en (state)
	WHERE state IS NOT NULL;

-- t_hs
CREATE INDEX idx_hs_sysid ON t_hs (sys_id);
CREATE INDEX idx_hs_callsign ON t_hs (callsign);

