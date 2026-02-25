CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_APINCDLNK(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPLICATION integer, 
            APPLINCIDKEY integer, 
            DELETED boolean, 
            INCIDENT integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 