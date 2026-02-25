CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PLANNING_PLANEMP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPLANEMPKEY integer, 
            APPLANKEY integer, 
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