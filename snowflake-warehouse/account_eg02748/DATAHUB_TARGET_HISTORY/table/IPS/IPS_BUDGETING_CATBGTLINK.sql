CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BUDGETING_CATBGTLINK(
            ADDBY varchar, 
            ADDDTTM datetime, 
            ALLOCKEY integer, 
            BGTDFNKEY integer, 
            BGTNO integer, 
            DELETED boolean, 
            EXPNOKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            SRCALLCKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 