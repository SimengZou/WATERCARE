CREATE OR REPLACE VIEW
  datahub_analytics.dw_ips_dim_accountserviceposition (account_service_key, account_service_position_key, added_by, added_datetime, address_key, 
  asset_type, consumption_percentage, graph_usage_flag, hide_readings_on_bill, meter_register_use, modified_by, modified_datetime, move_in_reading_key, 
  move_out_reading_key, position, start_datetime, stop_datetime, subtractive_flag, usedaysrds, use_uom, usghistinbilloutput, winter_average, etl_load_datetime)
AS SELECT  
  accountservicekey, accountservicepositionkey, addby, adddttm, addrkey, assettype, consumptionpercentage, graphusageflag,
  hidereadingsonbill, meterregisteruse, modby, moddttm, moveinreadingkey, moveoutreadingkey, position, startdttm, stopdttm,
  subtractiveflag, usedaysrds, useuom, usghistinbilloutput, winteravg, etl_load_datetime
FROM datahub_target.ips_billing_accountserviceposition;