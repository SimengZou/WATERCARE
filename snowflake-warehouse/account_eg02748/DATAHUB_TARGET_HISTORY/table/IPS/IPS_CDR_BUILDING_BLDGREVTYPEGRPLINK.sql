CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_BUILDING_BLDGREVTYPEGRPLINK(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            FILTERGRP integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            REVTYPE integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 