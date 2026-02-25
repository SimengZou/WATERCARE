CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_APGRP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APGRPKEY integer, 
            APGRPNAME varchar, 
            CDRPRODFAMILY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 