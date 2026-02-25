 CREATE OR REPLACE VIEW 
  datahub_analytics.dw_ips_dim_compwmtr (account_number, added_by, added_datetime, address_key, address_qualifier, area, asbuilt, average_monthly_usage,
  average_monthly_usage_uom, budget_number, building_floor, building_room, budget_key, bypass, comp_key, complex_key,
  component, district, expired_by, expire_date, gis_static, high_register, installed_date, interaction_key, last_reading_export_schedule_key,
  last_reading_import_schedule_key, location, low_register, main_key, map_no, meter_size, meter_size_uom, manufactured_date,
  manufacturer_key, modified_by, modified_datetime, model_number, out_for_activity_flag, own, position, parcel_key,
  pressure_zone, purchased_date, segment_key, serial_number, service_status, site_key, service_line_key, special_instructions,
  service_type, subarea, unit_description, unit_id, unit_type, usage_area_key, usage_date, usage_total, usage_total_uom,
  xcoordinate, ycoordinate, zcoordinate, etl_load_datetime, etl_source)
AS SELECT 
  acctno, addby, adddttm, addrkey, addrqual, area, asblt, avgmonusg, avgmonusguom, bgtno, bldgfloor, bldgroom, budgetkey, bypass,
  compkey, complexkey, component, district, expby, expdate, gisstatic, highreg, instdate, intkey, lastreadingexschdkey,
  lastreadingimschdkey, loc, lowreg, mainkey, mapno, metersz, meterszuom, mfgdate, mfgkey, modby, moddttm, modelno, outforactivityflag, 
  own, position, prclkey, preszone, purcdate, segkey, serno, servstat, sitekey, slkey, specinst,
  srvtype, subarea, unitdesc, unitid, unittype, usgareakey, usgdate, usgtot, usgtotuom, xcoord, ycoord, zcoord, ETL_LOAD_DATETIME , 'IPS'
FROM datahub_target.IPS_ASSETMANAGEMENT_WATER_COMPWMTR ;