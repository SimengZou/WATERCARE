CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CRM_REQTYPCAT(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CATKEY integer, 
            CATNAME varchar, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PARENT integer, 
            PORTALDESCRIPTION varchar, 
            PORTALORDER integer, 
            PORTALTITLE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 