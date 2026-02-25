 CREATE OR REPLACE VIEW 
  datahub_analytics.dw_ips_dim_address (added_by, added_datetime, address_status, address_key, allow_duplicate_services, area, block, cass_is_validation,
  cass_validation_description, cass_validation_date, cass_validation_status, city_limits, current_address, effective_date, expire_date,
  full_street_number, gps_x_coordinate, gps_y_coordinate, gps_z_coordinate, in_out_service_area, legal_description, legal_owner,
  lot, management_group, map_no, modified_by, modified_datetime, address_type, compass_direction, flat, house_number_high,
  house_number_sort_as, house_number_sort_as_high, house_number, post_code, street2_name, street2_type, street3_name,
  street3_type, state, street_name, street_type, suburb, option_a, option_b, option_c, option_d, property_use, subdivision_code,
  subdivision_description, township, version, etl_load_datetime, etl_source)
AS SELECT  
  addby, adddttm, addressstatus, addrkey, allowduplicateservices, area, block, cassisvalid, cassvalidationdesc, cassvalidationdt, cassvalidationstatus,
  citylimits, curraddr, effdate, expdate, fullstno, gpsx, gpsy, gpsz, inoutservicearea, legaldesc, legalowner, lot, managementgroup, mapno, modby, moddttm,
  nzaddrtype, nzcompdir, nzflat, nzhnohi, nzhnosortas, nzhnosortashi, nzhouseno, nzpostcode, nzst2name, nzst2type, nzst3name, nzst3type, nzstate, nzstname,
  nzsttype, nzsuburb, opta, optb, optc, optd, propertyuse, subdivcode, subdivdesc, township, version, etl_load_datetime, 'IPS'
FROM datahub_target.ips_property_address;