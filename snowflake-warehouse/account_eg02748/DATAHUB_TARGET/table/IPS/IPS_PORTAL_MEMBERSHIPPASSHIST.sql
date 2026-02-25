CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_PORTAL_MEMBERSHIPPASSHIST(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            MEMBERSHIPKEY integer, 
            MEMBERSHIPPASSHISTKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            PASSWORDCREATEDDATE datetime, 
            PASSWORDHASH varchar, 
            PASSWORDSALT varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 