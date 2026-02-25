CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_BILLRUNSCHEDULE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BILLRUNSCHEDULEKEY integer, 
            DELETED boolean, 
            INCREMENTVALUE integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            NUMBERPRIOR integer, 
            PRIORTIMEFRAME integer, 
            SCHEDULEKEY integer, 
            SUBSEQUENTRUNTYPE integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 