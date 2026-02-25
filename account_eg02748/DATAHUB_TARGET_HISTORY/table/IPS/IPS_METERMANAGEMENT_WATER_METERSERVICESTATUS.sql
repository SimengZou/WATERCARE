CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_METERMANAGEMENT_WATER_METERSERVICESTATUS(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            METERONOFF integer, 
            METERSERVICESTATUSKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            SERVICESTATUS varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 