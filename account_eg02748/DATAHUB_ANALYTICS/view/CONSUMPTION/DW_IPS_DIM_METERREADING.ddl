 CREATE OR REPLACE VIEW 
  datahub_analytics.dw_ips_dim_meterreading(added_by, added_datetime, audit_flag, billable_usage, billable_usage_in_cubic_feet, 
  comp_key, corrected_flag, custprob, cycle, estmate_flag, field_notes, final_flag, group_project, high_billable_usage, high_billable_usage_in_cubic_feet, 
  high_reading, high_usage, histkey, initial_flag, inspection_key, modified_by, modified_datetime, no_read_flag, prob_code_work_order, 
  prob_code_service_request_no, prob_code, reader_code_service_request_no, read_by, read_datetime, reader_code, reader_code_work_order, reading, 
  reading_key, read_reason, read_source, read_value1, read_value2, read_value3, read_value4, ready_flag, usage, etl_load_datetime)
AS SELECT 
  addby, adddttm, auditflag, blusage, blusagecubicft, compkey, corrflag, custprob, cycle, estmflag, fieldnotes, 
  finalflag, grpproj, highblusage, highblusageincubicfeet, highreading, highusage, histkey, initflag, inspkey, modby,
  moddttm, noreadflag, probbcodewo, probcodesrvreqno, problemcode, rdrcodesrvreqno, readby, readdttm, readercode,
  readercodewo, reading, readingkey, readreas, readsrc, readval1, readval2, readval3, readval4, readyflag, usage, etl_load_datetime
FROM datahub_target.IPS_ASSETMANAGEMENT_WATER_METERREADING;