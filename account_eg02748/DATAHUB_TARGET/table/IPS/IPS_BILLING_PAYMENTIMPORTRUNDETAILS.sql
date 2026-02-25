CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_PAYMENTIMPORTRUNDETAILS(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BATCHKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PAYMENTEXCEPTIONCOUNT integer, 
            PAYMENTIMPORTRUNDETAILSKEY integer, 
            PAYMENTIMPORTRUNKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 