CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_APCUST(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APCUSTKEY integer, 
            APKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            SERVNO integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 