 CREATE OR REPLACE VIEW 
  datahub_analytics.dw_ips_dim_xwatermeterdetail(added_datetime, comp_key, modified_datetime, meter_zero_code, etl_load_datetime)
AS SELECT 
  adddttm, compkey, moddttm, meterzerocode, etl_load_datetime
FROM DATAHUB_TARGET.IPS_WSLASSETWATER_XWATERMETERDETAIL;