CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_PROJECT_PROJLICREQ(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJKEY integer, 
            APPROJLICREQKEY integer, 
            DELETED boolean, 
            GROUPNO integer, 
            LICTYPE varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 