CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_BUILDING_BLDGFILTERGRP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BLDGFILTERGRPKEY integer, 
            DATALAKE_DELETED boolean, 
            GRPTYPE integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            NAME varchar, 
            SHOWALLCODE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 