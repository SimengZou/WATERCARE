CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_ASSETMANAGEMENT_ASSETVALUATION_ASSVALGRPASSVALUE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            ASSVALKEY integer, 
            DATALAKE_DELETED boolean, 
            GROUPITEMKEY integer, 
            GROUPKEY integer, 
            INDEXNO integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 