create or replace stream STREAM_SM_CURATED_ALARMSEVENTS 
on table DATAHUB_LANDING.SM_CURATED_ALARMSEVENTS append_only = true;