CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_APVLTN(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APKEY integer, 
            APVLTNKEY integer, 
            BLDGAREA numeric(38, 10), 
            CALCVLTN numeric(38, 10), 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            SUVLTNKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 