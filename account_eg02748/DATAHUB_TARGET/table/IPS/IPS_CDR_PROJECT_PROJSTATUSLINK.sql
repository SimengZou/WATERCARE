CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PROJECT_PROJSTATUSLINK(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJDEFNKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            STATUSCODE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 