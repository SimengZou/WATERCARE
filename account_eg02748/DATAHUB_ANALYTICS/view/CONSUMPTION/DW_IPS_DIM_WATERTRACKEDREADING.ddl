 CREATE OR REPLACE VIEW 
  datahub_analytics.dw_ips_dim_watertrackedreading(account_key, account_service_position_key, added_by, added_datetime, bill_key, bill_type_key, modified_by, 
  modified_datetime, reading_key, water_tracked_reading_key, etl_load_datetime)
AS SELECT 
  accountkey, acctservicepositionkey, addby, adddttm, billkey, billtypekey, modby, moddttm, readingkey, watertrackedreadingkey, etl_load_datetime
FROM DATAHUB_TARGET.IPS_BILLING_WATERTRACKEDREADING;