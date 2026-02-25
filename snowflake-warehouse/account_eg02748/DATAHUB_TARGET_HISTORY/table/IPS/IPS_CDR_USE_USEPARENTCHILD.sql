CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_USE_USEPARENTCHILD(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APUSEPARENTCHILDKEY integer, 
            CAPACITY varchar, 
            CHILDKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PARENTKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 