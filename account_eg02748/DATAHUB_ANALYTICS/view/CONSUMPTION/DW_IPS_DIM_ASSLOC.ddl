 CREATE OR REPLACE VIEW 
  datahub_analytics.dw_ips_dim_assloc (added_by, added_datetime, address_key, assloc_key, comments, comments_search, comp_key, installed_datetime,
  modified_by, modified_datetime, position, removed_datetime, etl_load_datetime)
AS SELECT  
  addby, adddttm, addrkey, asslockey, comments, comments_search, compkey,
  instdt, modby, moddttm, position, remdt, etl_load_datetime
FROM datahub_target.ips_assetmanagement_assloc;