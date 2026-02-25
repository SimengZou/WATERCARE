CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_ARRANGEMENTBILLTYPE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            ARRANGEMENTBILLTYPEKEY integer, 
            BILLTYPEKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PAYMENTARRANGEMENTKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 