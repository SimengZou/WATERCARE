CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5FAILURES_DELETED(
            FAL_CODE varchar, 
            FAL_CREATED datetime, 
            FAL_DESC varchar, 
            FAL_ENABLEWORKORDERS varchar, 
            FAL_GEN varchar, 
            FAL_GROUP varchar, 
            FAL_LASTSAVED datetime, 
            FAL_NOTUSED varchar, 
            FAL_PARTFAILURE varchar, 
            FAL_UPDATECOUNT numeric(38, 10), 
            FAL_UPDATED datetime, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 