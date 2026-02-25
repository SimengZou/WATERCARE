CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PROJECT_PROJPROCSTATECHKCOND(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROJCONDTYPEKEY integer, 
            APPROJPROCSTATECHKCONDKEY integer, 
            APPROJPROCSTATECHKKEY integer, 
            APRV varchar, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 