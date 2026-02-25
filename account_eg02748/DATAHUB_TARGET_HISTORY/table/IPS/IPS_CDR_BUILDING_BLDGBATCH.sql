CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_BUILDING_BLDGBATCH(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGBATCHKEY integer, 
            APBLDGDEFNKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            SUBDIVCODE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 