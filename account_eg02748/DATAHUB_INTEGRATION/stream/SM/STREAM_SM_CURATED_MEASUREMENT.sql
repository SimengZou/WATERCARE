create or replace stream STREAM_SM_CURATED_MEASUREMENT 
on table DATAHUB_LANDING.SM_CURATED_MEASUREMENT append_only = true show_initial_rows = true;