CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5ALERTMAIL(
            ALM_ALERT varchar, 
            ALM_DELAY numeric(38, 10), 
            ALM_DELAYUOM varchar, 
            ALM_LASTSAVED datetime, 
            ALM_TEMPLATE varchar, 
            ALM_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 