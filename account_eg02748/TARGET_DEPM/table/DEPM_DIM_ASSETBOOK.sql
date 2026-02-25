CREATE OR REPLACE TABLE TARGET_DEPM.DEPM_DIM_ASSETBOOK(
            ElementName varchar, 
            LongName varchar, 
            Name varchar, 
            Parent varchar, 
            Timestamp datetime, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 