CREATE OR REPLACE TABLE TARGET_HISTORY_DEPM.DEPM_DIM_CAPEX_PERIOD_SNAPSHOT(
            Description varchar, 
            ElementName varchar, 
            Parent varchar, 
            Sort varchar, 
            Timestamp datetime, 
            Type varchar, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 