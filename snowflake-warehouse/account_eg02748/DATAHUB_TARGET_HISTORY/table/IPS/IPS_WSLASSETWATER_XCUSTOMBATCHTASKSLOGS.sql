CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLASSETWATER_XCUSTOMBATCHTASKSLOGS(
            ADDBY varchar, 
            ADDDTTM datetime, 
            COMMENTS varchar, 
            DATALAKE_DELETED boolean, 
            ENTITYKEY varchar, 
            ENTITYNAME varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            TASKNAME varchar, 
            VARIATION_ID integer, 
            XCUSTOMBATCHTASKSLOGSKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 