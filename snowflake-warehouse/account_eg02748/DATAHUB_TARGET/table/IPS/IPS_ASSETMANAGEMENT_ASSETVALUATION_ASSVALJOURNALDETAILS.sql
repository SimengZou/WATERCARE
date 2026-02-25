CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALJOURNALDETAILS(
            ADDBY varchar, 
            ADDDTTM datetime, 
            AMOUNT numeric(38, 10), 
            ASSVALJOURNALDETAILSKEY integer, 
            ASSVALKEY integer, 
            ASSVALRUNKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            TRANSACTIONDATE datetime, 
            TRANSACTIONTYPE integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 