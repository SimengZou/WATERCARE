CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_BUILDING_BLDGPROCSTATECHKCOND(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGCONDTYPEKEY integer, 
            APBLDGPROCSTATECHKCONDKEY integer, 
            APBLDGPROCSTATECHKKEY integer, 
            APRV varchar, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 