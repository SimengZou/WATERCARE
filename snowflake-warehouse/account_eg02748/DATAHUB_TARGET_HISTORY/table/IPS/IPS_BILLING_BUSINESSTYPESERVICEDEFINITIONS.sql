CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_BUSINESSTYPESERVICEDEFINITIONS(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BUSINESSTYPECODE varchar, 
            BUSINESSTYPESRVDEFNKEY integer, 
            DEFAULTAVERAGEUSAGE numeric(38, 10), 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            SERVICEDEFINITIONKEY integer, 
            UNITOFMEASURE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 