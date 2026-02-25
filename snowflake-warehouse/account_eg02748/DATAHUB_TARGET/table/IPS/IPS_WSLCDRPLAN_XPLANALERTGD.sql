CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLCDRPLAN_XPLANALERTGD(
            ADDBY varchar, 
            ADDDTTM datetime, 
            ALERT varchar, 
            COMMENTS varchar, 
            DELETED boolean, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            XPLANALERTCR integer, 
            XPLANALERTGDKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 