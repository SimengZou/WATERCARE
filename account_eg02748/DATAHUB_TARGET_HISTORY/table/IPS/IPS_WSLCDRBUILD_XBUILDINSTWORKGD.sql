CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRBUILD_XBUILDINSTWORKGD(
            ADDBY varchar, 
            ADDDTTM datetime, 
            COMPLETEDDATE datetime, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VALUE varchar, 
            VARIATION_ID integer, 
            WORKITEM varchar, 
            XBUILDINSTALLDPKEY integer, 
            XBUILDINSTWORKGDKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 