CREATE OR REPLACE TABLE TARGET_DEPM.DEPM_DIM_OPEX_VERSION(
            Active varchar, 
            ElementName varchar, 
            Name varchar, 
            Parent varchar, 
            ShortName varchar, 
            Timestamp datetime, 
            WorkflowEnabled varchar, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 