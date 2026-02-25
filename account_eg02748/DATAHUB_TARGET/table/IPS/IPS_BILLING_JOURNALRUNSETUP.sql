CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_JOURNALRUNSETUP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            JOURNALRUNSETUPKEY integer, 
            JOURNALSENABLEDFLAG varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            NUMOFTRANSPERCOMMIT integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 