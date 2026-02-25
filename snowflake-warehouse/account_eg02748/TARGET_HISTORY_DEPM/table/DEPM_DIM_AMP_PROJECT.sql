CREATE OR REPLACE TABLE TARGET_HISTORY_DEPM.DEPM_DIM_AMP_PROJECT_SNAPSHOT(
            AMPCode varchar, 
            Active varchar, 
            BusinessArea varchar, 
            EMCode varchar, 
            ElementName varchar, 
            Growth varchar, 
            LevelofService varchar, 
            Name varchar, 
            Parent varchar, 
            ProgramManager varchar, 
            ProjectManager varchar, 
            Replacement varchar, 
            ShortName varchar, 
            Timestamp datetime, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 