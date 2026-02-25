CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_USE_USEVLTN(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APUSEKEY integer, 
            APUSEVLTNKEY integer, 
            CALCVLTN numeric(38, 10), 
            DELETED boolean, 
            DESCRIPT varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            SUVLTNKEY integer, 
            USEAREA numeric(38, 10), 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 