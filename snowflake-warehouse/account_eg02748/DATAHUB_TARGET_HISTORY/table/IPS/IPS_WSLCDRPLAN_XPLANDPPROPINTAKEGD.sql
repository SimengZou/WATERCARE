CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRPLAN_XPLANDPPROPINTAKEGD(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            XPLANDPPROPINTAKEGDKEY integer, 
            XPLANDPPROPINTAKEKEY integer, 
            ZONE varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 