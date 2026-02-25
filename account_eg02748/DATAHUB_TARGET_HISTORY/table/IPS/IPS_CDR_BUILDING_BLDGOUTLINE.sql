CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_BUILDING_BLDGOUTLINE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGPROCESSSTATEKEY integer, 
            BLDGOUTLINEKEY integer, 
            DATALAKE_DELETED boolean, 
            DISPLAYNAME varchar, 
            DISPLAYORDER integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            PORTALDESC varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 