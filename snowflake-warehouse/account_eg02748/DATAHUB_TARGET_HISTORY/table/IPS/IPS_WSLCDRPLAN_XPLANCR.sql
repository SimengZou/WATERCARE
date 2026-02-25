CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRPLAN_XPLANCR(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPLANKEY integer, 
            APPLICAITONALERT varchar, 
            DATEOFAPPLICATION datetime, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PROPOSALNAME varchar, 
            VARIATION_ID integer, 
            XPLANCRKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 