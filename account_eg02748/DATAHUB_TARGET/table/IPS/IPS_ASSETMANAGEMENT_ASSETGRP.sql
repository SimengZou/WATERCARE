CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_ASSETMANAGEMENT_ASSETGRP(
            ACCTGRP varchar, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            GROUPDESC varchar, 
            GROUPID varchar, 
            GROUPKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            ROUTEFLAG varchar, 
            ROUTETIME varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 