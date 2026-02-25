CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PROJECT_PROJLICCHK(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJKEY integer, 
            APPROJLICCHKKEY integer, 
            APTRADEKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            REQUIREDLICENSE integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 