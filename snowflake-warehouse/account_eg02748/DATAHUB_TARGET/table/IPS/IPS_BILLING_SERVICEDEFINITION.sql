CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_SERVICEDEFINITION(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DESCRIPTION varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            SERVICE varchar, 
            SERVICECATEGORY integer, 
            SERVICEDEFINITIONKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 