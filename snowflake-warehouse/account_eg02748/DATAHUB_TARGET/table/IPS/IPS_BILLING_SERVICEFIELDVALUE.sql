CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_SERVICEFIELDVALUE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DESCRIPTION varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            SERVICEFIELDKEY integer, 
            SERVICEFIELDVALUEKEY integer, 
            VALUE numeric(38, 10), 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 