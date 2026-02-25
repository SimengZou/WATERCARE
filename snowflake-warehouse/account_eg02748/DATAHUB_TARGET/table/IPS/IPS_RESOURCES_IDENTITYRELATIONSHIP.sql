CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_RESOURCES_IDENTITYRELATIONSHIP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DESTINATION integer, 
            DESTINATIONRELATIONSHIP varchar, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            IDENTITYRELATIONSHIPKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            SOURCE integer, 
            SOURCERELATIONSHIP varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 