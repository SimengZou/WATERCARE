CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PROJECT_PROJVLTN(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJKEY integer, 
            BLDGAREA numeric(38, 10), 
            CALCVLTN numeric(38, 10), 
            DELETED boolean, 
            DESCRIPT varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            MULT numeric(38, 10), 
            PROJVLTNKEY integer, 
            SUVLTNKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 