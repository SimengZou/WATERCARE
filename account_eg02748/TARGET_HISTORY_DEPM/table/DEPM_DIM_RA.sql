CREATE OR REPLACE TABLE TARGET_HISTORY_DEPM.DEPM_DIM_RA_SNAPSHOT(
            ElementName varchar, 
            Parent varchar, 
            Timestamp datetime, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 