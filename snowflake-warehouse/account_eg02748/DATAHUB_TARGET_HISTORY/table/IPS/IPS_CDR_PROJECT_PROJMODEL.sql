CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_PROJECT_PROJMODEL(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJKEY integer, 
            BASEMODELKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            MODELTITLE varchar, 
            MODELTYPE varchar, 
            PROJMODELKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 