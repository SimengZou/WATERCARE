CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5ALERTGENERATEWO_DELETED(
            AGW_ALERT varchar, 
            AGW_GENERATETHROUGHFIELDID numeric(38, 10), 
            AGW_LASTSAVED datetime, 
            AGW_TYPE varchar, 
            AGW_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 