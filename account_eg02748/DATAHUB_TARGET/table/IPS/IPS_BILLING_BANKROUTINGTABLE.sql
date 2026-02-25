CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_BANKROUTINGTABLE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BNKRTTBLKEY integer, 
            DDBANK varchar, 
            DDBRANCH varchar, 
            DDROUTNO varchar, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 