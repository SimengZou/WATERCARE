CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_BUILDING_BLDGAPPLWORKGRPLINK(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPLWORKGROUPLINKKEY integer, 
            APPLWORKGRP integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            WORKOCCFILTERGRP integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 