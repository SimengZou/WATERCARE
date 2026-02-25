CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLSERVICEREQUEST_XSRSTATUSDETAILS(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            SERVNO integer, 
            SRSTATUS varchar, 
            VARIATION_ID integer, 
            XSRSTATUSDETAILSKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 