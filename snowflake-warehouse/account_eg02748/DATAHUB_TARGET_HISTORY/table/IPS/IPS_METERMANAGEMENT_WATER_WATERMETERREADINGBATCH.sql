CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_METERMANAGEMENT_WATER_WATERMETERREADINGBATCH(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BATCHDATE datetime, 
            COMPAREOPTIONS integer, 
            CYCLE varchar, 
            DATALAKE_DELETED boolean, 
            EXCLUDEMETERS varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            ROUTEKEY integer, 
            VARIATION_ID integer, 
            WATERMETERREADINGBATCHKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 