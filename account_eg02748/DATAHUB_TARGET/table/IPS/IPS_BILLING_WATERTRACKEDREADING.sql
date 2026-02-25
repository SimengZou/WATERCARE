CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_WATERTRACKEDREADING(
            ACCOUNTKEY integer, 
            ACCTSERVICEPOSITIONKEY integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            BILLKEY integer, 
            BILLTYPEKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            READINGKEY integer, 
            VARIATION_ID integer, 
            WATERTRACKEDREADINGKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 