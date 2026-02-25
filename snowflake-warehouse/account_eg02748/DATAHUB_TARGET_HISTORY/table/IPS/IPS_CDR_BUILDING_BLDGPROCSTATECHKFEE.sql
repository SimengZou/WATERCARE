CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_BUILDING_BLDGPROCSTATECHKFEE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGFEETYPEKEY integer, 
            APBLDGPROCSTATECHKFEEKEY integer, 
            APBLDGPROCSTATECHKKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            STAT varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 