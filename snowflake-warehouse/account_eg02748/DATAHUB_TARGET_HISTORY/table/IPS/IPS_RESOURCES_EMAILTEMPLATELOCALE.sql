CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_RESOURCES_EMAILTEMPLATELOCALE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            EMAILTEMPLATEKEY integer, 
            EMAILTEMPLATELOCALEKEY integer, 
            LANGUAGE varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            SUBJECT varchar, 
            USEASDEFAULT varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 