CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_METERSIZE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            ASSETTYPE integer, 
            DELETED boolean, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            METERSIZE numeric(38, 10), 
            METERSIZEKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 