CREATE OR REPLACE TABLE TARGET_HISTORY_DEPM.DEPM_AMP_SNAPSHOT(
            AMPBusinessArea varchar, 
            AMPEMCodes varchar, 
            AMPMeas varchar, 
            AMPPeriod varchar, 
            AMPProject varchar, 
            AMPVersion varchar, 
            AMPYear varchar, 
            AMPcodes varchar, 
            Timestamp datetime, 
            Value varchar, ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 