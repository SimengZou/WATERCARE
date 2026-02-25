CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_SERVICEFIELDRBOPTION(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DEFAULTFLAG varchar, 
            DELETED boolean, 
            DISPLAYORDER integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            OPTIONTEXT varchar, 
            SERVICEFIELDKEY integer, 
            SERVICEFIELDRBOPTIONKEY integer, 
            VALUE numeric(38, 10), 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 