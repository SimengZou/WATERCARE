CREATE OR REPLACE TABLE TARGET_HISTORY_DEPM.DEPM_R100_SNAPSHOT(
            Category varchar, 
            Measure varchar, 
            Project varchar, 
            RiskActionID varchar, 
            RiskID varchar, 
            Timestamp datetime, 
            Value varchar, 
            Version varchar, 
            WBS varchar, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 