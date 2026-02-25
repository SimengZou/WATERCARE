DROP TABLE IF EXISTS datahub_analytics.landing_dw_water_meter_dma;
CREATE TABLE datahub_analytics.landing_dw_water_meter_dma
(
	dw_water_meters_dma_key Integer IDENTITY NOT NULL
	,dma_code VARCHAR(256)  
	,dma_name VARCHAR(256)  
	,compkey VARCHAR(256)   
	,hacompkey VARCHAR(256)  
	,meter_no VARCHAR(256)   
	,etl_src_file_from VARCHAR(100)   
	,etl_load_datetime TIMESTAMP 
)
;