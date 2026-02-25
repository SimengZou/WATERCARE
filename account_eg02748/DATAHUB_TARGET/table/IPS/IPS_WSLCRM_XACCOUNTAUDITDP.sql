CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLCRM_XACCOUNTAUDITDP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            SERVNO integer, 
            VARIATION_ID integer, 
            XACCOUNTAUDITDPKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 