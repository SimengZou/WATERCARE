CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_METERMANAGEMENT_WATER_USGESTEXCEPTION(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            EXCEPTIONDESCRIPTION varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            READINGKEY integer, 
            USGESTEXCEPTIONKEY integer, 
            USGESTHISTKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 