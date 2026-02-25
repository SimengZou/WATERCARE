CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_FEATURESWITCHES(
            ACTIVATEDFLAG varchar, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            FEATURE integer, 
            FEATURESWITCHESKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 