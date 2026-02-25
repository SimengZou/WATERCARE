CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLCRM_XVALUATIONSAUDITDP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            SERVNO integer, 
            VARIATION_ID integer, 
            XVALUATIONSAUDITDPKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 