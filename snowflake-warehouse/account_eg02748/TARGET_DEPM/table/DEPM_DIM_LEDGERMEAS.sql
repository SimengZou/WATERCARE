CREATE OR REPLACE TABLE TARGET_DEPM.DEPM_DIM_LEDGERMEAS(
            ElementName varchar, 
            FORMAT_STRING varchar, 
            Parent varchar, 
            Timestamp datetime, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 