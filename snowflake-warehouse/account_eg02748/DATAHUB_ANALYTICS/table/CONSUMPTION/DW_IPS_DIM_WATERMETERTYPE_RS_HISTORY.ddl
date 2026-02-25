DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_watermetertype_rs_history;
CREATE TABLE datahub_analytics.dw_ips_dim_watermetertype_rs_history
(
	dim_ips_watermetertype_key INTEGER
	,added_datetime TIMESTAMP    
	,code VARCHAR(10) NOT NULL  
	,lowregnumberdials INTEGER   
	,modified_datetime TIMESTAMP    
	,etl_load_datetime TIMESTAMP    
)
;
