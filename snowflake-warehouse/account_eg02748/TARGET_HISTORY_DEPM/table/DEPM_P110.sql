CREATE OR REPLACE TABLE TARGET_HISTORY_DEPM.DEPM_P110_SNAPSHOT(
            PM110 varchar, 
            Project varchar, 
            Timestamp datetime, 
            Value varchar, 
            WBS varchar, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 