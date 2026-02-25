CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PROJECT_PROJEMP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJEMPKEY integer, 
            APPROJKEY integer, 
            CAPACITY varchar, 
            DATALAKE_DELETED boolean, 
            EMPID varchar, 
            FROMDTTM datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            TODTTM datetime, 
            TOTALHRS numeric(38, 10), 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 