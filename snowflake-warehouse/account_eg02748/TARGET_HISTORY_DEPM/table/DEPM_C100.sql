CREATE OR REPLACE TABLE TARGET_HISTORY_DEPM.DEPM_C100_SNAPSHOT(
            Asset varchar, 
            AssetBook varchar, 
            CM100 varchar, 
            Project varchar, 
            Timestamp datetime, 
            Value varchar, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 