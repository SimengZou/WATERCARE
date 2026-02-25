CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5FUTUREDUPLICATES(
            FDP_DATE datetime, 
            FDP_LASTSAVED datetime, 
            FDP_PPOPK numeric(38, 10), 
            FDP_SEQNO numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 