 CREATE OR REPLACE VIEW 
  datahub_analytics.dw_ips_dim_meterzerocodes (added_datetime, code, description, modified_datetime, etl_load_datetime)
AS SELECT 
  adddttm, code, descript, moddttm, ETL_LOAD_DATETIME 
FROM DATAHUB_TARGET.IPS_WSLASSETWATER_METERZEROCODES;