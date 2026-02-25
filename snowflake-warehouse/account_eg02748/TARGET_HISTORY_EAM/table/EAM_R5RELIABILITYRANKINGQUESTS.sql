CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5RELIABILITYRANKINGQUESTS_DELETED(
            RRQ_LANG varchar, 
            RRQ_LASTSAVED datetime, 
            RRQ_LEVELPK varchar, 
            RRQ_QUESTION varchar, 
            RRQ_TRANS varchar, 
            RRQ_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 