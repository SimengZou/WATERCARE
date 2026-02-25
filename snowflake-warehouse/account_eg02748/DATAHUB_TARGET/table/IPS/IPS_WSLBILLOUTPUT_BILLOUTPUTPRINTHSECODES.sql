CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLBILLOUTPUT_BILLOUTPUTPRINTHSECODES(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BILLKEY integer, 
            BILLOUTPUTPRINTHSECODESKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PRINTHOUSECODE varchar, 
            PRINTHOUSEUSER varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 