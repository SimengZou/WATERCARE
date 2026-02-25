CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_RESOURCES_EMPRATEHIST(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BENEFITRATE numeric(38, 10), 
            COMMENTS varchar, 
            DATALAKE_DELETED boolean, 
            EFFECTIVEDATE datetime, 
            EMPLOYEE varchar, 
            EMPRATEHISTKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            RATE numeric(38, 10), 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 