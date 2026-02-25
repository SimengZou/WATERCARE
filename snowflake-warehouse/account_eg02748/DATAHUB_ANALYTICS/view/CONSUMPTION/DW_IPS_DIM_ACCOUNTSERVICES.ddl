CREATE OR REPLACE VIEW
  datahub_analytics.dw_ips_dim_accountservices (account_key, account_service_key, added_by, added_datetime, address_service_key, area, 
  consumption_percentage, impervious_percentage, modified_by, modified_datetime, service_class1, service_class2, service_field1_value,
  service_field2_value, service_field3_value, service_options_key, start_datetime, status, status_datetime, stop_datetime, suspend_from_date,
  suspend_to_date, winter_avg_calculation_date, winter_average, etl_load_datetime)
AS SELECT 
  accountkey, accountservicekey, addby, adddttm, addressservicekey, area, consumptionpercentage, imperviouspercentage, modby, moddttm, 
  serviceclass1, serviceclass2, servicefield1value, servicefield2value, servicefield3value, serviceoptionskey, startdttm, status, 
  statusdttm, stopdttm, suspendfromdate, suspendtodate, winavgcaldate, winteravg, etl_load_datetime
  FROM datahub_target.ips_billing_accountservice;