CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_RESOURCES_CREDITRATINGSETUP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CREDITRATINGSETUPKEY integer, 
            DATALAKE_DELETED boolean, 
            LATEPAYMENTPOINTSCODE varchar, 
            LOOKBACKPERIODDAYS integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 