CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLTRADEBILLOUTPUT_TRADEBILLOUTPUTTASKLOG(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BILLKEY integer, 
            COMMENTS varchar, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            TRADEBILLOUTPUTTASKLOGKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 