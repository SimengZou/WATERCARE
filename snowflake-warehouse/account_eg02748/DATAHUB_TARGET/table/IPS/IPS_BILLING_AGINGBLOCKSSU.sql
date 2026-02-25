CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_AGINGBLOCKSSU(
            ADDBY varchar, 
            ADDDTTM datetime, 
            AGINGBLOCK1 integer, 
            AGINGBLOCK2 integer, 
            AGINGBLOCK3 integer, 
            AGINGBLOCK4 integer, 
            AGINGBLOCKSKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 