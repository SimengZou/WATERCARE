CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_LINEITEMTRACKEDREAD(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            LINEITEMKEY integer, 
            LINEITEMTRACKEDREADKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            SEWERTRACKEDREADINGKEY integer, 
            STORMTRACKEDREADING integer, 
            VARIATION_ID integer, 
            WATERTRACKEDREADING integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 