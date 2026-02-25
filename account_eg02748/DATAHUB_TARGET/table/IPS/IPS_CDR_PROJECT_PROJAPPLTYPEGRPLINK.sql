CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PROJECT_PROJAPPLTYPEGRPLINK(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPLTYPEKEY integer, 
            DATALAKE_DELETED boolean, 
            FILTERGRP integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 