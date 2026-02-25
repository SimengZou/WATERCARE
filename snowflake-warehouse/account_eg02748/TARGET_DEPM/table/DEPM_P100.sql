CREATE OR REPLACE TABLE TARGET_DEPM.DEPM_P100(
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