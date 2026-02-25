CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_APBILLACCTCHNGLOG(
            ACCTCHANGETRANSFERTYPE integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBILLACCTCHNGLOGKEY integer, 
            APKEY integer, 
            DELETED boolean, 
            FROMBILLACCTKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            TOBILLACCTKEY integer, 
            VARIATION_ID integer, 
            XFERTRNFLAG varchar, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 