CREATE OR REPLACE TABLE TARGET_DEPM.DEPM_DIM_CAPEX_VERSION(
            DESCRIPTION varchar, 
            ElementName varchar, 
            Parent varchar, 
            Timestamp datetime, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 