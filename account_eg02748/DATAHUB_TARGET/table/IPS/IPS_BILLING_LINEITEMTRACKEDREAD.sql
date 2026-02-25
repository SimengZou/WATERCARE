CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_LINEITEMTRACKEDREAD(
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
            etl_load_metadatafilename varchar
            ); 