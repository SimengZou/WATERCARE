CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_PLANNING_PLANHEARLOG(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            LOGBY varchar, 
            LOGDTTM datetime, 
            LOGTYPE varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            PLANHEARKEY integer, 
            PLANHRLOGKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 