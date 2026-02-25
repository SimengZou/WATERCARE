CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRPROJ_XPROJNETCONQUOTEDP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJREVDTLKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            QUOTEDUEDATE datetime, 
            SENDTOCUSTOMER varchar, 
            VARIATION_ID integer, 
            XPROJNETCONQUOTEDPKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 