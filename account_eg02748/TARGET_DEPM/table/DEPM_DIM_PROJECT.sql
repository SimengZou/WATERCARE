CREATE OR REPLACE TABLE TARGET_DEPM.DEPM_DIM_PROJECT(
            ElementName varchar, 
            HLevel varchar, 
            LongName varchar, 
            Name varchar, 
            Parent varchar, 
            Timestamp datetime, 
            Type varchar, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 