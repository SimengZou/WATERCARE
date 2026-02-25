-- datahub_analytics.dw_ips_dim_acct_comp_actsrv definition

DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_acct_comp_actsrv;

CREATE TABLE IF NOT EXISTS datahub_analytics.dw_ips_dim_acct_comp_actsrv

(
	account_key INTEGER
	,comp_key INTEGER 
	,account_service_key INTEGER 
)

;
-- ALTER TABLE datahub_analytics.dw_ips_dim_acct_comp_actsrv owner to wslmaster;