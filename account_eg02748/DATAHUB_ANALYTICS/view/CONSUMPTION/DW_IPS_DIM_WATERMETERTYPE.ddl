 CREATE OR REPLACE VIEW 
  datahub_analytics.dw_ips_dim_watermetertype (added_datetime, code, lowregnumberdials, modified_datetime, etl_load_datetime)
AS SELECT 
  adddttm, code, lowregnumberdials, moddttm, etl_load_datetime
FROM DATAHUB_TARGET.IPS_ASSETMANAGEMENT_WATER_WATERMETERTYPE;