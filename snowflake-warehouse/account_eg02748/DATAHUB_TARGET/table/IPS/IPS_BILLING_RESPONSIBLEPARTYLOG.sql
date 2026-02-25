CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_RESPONSIBLEPARTYLOG(
            ACCOUNTKEY integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            IDKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            RESPONSIBLEPARTYKEY integer, 
            RESPONSIBLEPARTYLOGKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 