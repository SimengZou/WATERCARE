DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_assloc_rs_history;
CREATE TABLE datahub_analytics.dw_ips_dim_assloc_rs_history
(
	dim_ips_assloc_key INTEGER
	,added_by VARCHAR(30)   
	,added_datetime TIMESTAMP   
	,address_key INTEGER   
	,assloc_key INTEGER NOT NULL  
	,comments VARCHAR(8000)   
	,comments_search VARCHAR(2000)   
	,comp_key INTEGER NOT NULL  
	,installed_datetime TIMESTAMP   
	,modified_by VARCHAR(30)   
	,modified_datetime TIMESTAMP   
	,position INTEGER   
	,removed_datetime TIMESTAMP   
	,etl_load_datetime TIMESTAMP   
)
;