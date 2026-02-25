CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PLANNING_PLANPROJAPPL(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPLANKEY integer, 
            APPLANPROJAPPLKEY integer, 
            APPROJKEY integer, 
            COMMENTS varchar, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 