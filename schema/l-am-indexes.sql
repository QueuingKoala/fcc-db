-- FCC DB indexes

-- t_hd
CREATE INDEX idx_hd_ulsfileno ON t_hd (uls_fileno)
	WHERE uls_fileno IS NOT NULL;
CREATE INDEX idx_hd_callsign ON t_hd (callsign);
CREATE INDEX idx_hd_licstatus ON t_hd (license_status);

-- t_am
CREATE INDEX idx_am_ulsfileno ON t_am (uls_fileno)
	WHERE uls_fileno IS NOT NULL;
CREATE INDEX idx_am_callsign ON t_am (callsign);

-- t_en
CREATE INDEX idx_en_ulsfileno ON t_en (uls_fileno)
	WHERE uls_fileno IS NOT NULL;
CREATE INDEX idx_en_callsign ON t_en (callsign);
CREATE INDEX idx_en_state ON t_en (state)
	WHERE state IS NOT NULL;
