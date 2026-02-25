CREATE OR REPLACE TABLE TARGET_HISTORY_DEPM.DEPM_DIM_LABOURMEAS_SNAPSHOT(
            ElementName varchar, 
            Name varchar, 
            Parent varchar, 
            ShortName varchar, 
            Timestamp datetime, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 