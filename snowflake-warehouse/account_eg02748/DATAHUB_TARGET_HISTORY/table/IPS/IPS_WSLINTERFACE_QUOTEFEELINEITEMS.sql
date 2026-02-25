CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLINTERFACE_QUOTEFEELINEITEMS(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            FEEAMOUNT numeric(38, 10), 
            FEEQTY numeric(38, 10), 
            FEEVALUE numeric(38, 10), 
            MODBY varchar, 
            MODDTTM datetime, 
            QUOTEFEELINEITEMSKEY integer, 
            QUOTEGENERATIONSTAGINGKEY integer, 
            SERVICENAME varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 