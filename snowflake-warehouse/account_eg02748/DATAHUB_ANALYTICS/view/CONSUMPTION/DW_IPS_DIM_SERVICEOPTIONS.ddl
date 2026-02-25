 CREATE OR REPLACE VIEW 
  datahub_analytics.dw_ips_dim_serviceoptions (added_datetime, asset_type, code, modified_datetime, service_options_key, etl_load_datetime)
AS SELECT 
  adddttm, assettype, code, moddttm, serviceoptionskey, etl_load_datetime
FROM datahub_target.ips_billing_serviceoptions;