DROP TABLE IF EXISTS datahub_staging.dw_stage_dma_supply_bsp_meter_zone;
CREATE TABLE datahub_staging.dw_stage_dma_supply_bsp_meter_zone
(
	name VARCHAR(256) 
	,meter_type VARCHAR(256)
	,area VARCHAR(256)  
	,facility_code VARCHAR(256)  
	,tag VARCHAR(256) 
	,add_subtract VARCHAR(256)  
	,district_metered_area VARCHAR(256) 
	,dma_code VARCHAR(256) 
	,district_zone VARCHAR(256) 
	,dz_code VARCHAR(256) 
	,water_balance_zone VARCHAR(256)
	,wbz_code VARCHAR(256)  
)
;
