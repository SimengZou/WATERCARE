CREATE OR REPLACE TABLE TARGET_HISTORY_DEPM.DEPM_P100_SNAPSHOT(
            PM100 varchar, 
            Period varchar, 
            Project varchar, 
            Timestamp datetime, 
            Value numeric(38, 10), 
            Version varchar, 
            WBS varchar, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 