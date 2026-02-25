DROP TABLE IF EXISTS datahub_staging.dw_stage_dma_supply_bsp_data;
CREATE TABLE datahub_staging.dw_stage_dma_supply_bsp_data
(
	meter_id VARCHAR(50)   
	,meter_location VARCHAR(256)  
	,cal_year INTEGER   
	,cal_month_no INTEGER 
	,cal_day INTEGER 
	,value DOUBLE PRECISION
)
;

