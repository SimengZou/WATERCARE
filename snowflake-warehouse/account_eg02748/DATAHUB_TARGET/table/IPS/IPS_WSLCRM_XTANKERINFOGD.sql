CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLCRM_XTANKERINFOGD(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            KEYISSUEDATE datetime, 
            KEYNUMBER varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            XTANKERINFODPKEY integer, 
            XTANKERINFOGDKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 