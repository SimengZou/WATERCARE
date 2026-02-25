CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_BUILDING_BLDGPROCSTATECHKREVIEW(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGPROCSTATECHKKEY integer, 
            APBLDGPROCSTATECHKREVKEY integer, 
            APBLDGREVIEWTYPEKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            RESULT varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 