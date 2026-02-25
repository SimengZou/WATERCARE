CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_ASSETMANAGEMENT_ASSETPOSITION(
            ADDBY varchar, 
            ADDDTTM datetime, 
            ADDRKEY integer, 
            ASSETPOSITIONKEY integer, 
            COMPTYPE integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            POSITIONS integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 