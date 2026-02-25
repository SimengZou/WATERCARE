CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRBUILD_XBUILDREVWCR(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGREVIEWKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            ONHOLD varchar, 
            SENDESTIMATETOCONTRACTOR varchar, 
            VARIATION_ID integer, 
            XBUILDREVWCRKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 