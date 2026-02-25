CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_RESOURCES_NOTIFICATION(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            DETAILS varchar, 
            IDKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            NOTIFICATIONKEY integer, 
            NOTIFICATIONMETHOD varchar, 
            NOTIFICATIONTYPE varchar, 
            REFERENCENUMBER integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 