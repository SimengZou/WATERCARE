CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5ALERTGRIDPARAMS_DELETED(
            AGP_ALERT varchar, 
            AGP_DVALUE datetime, 
            AGP_LASTSAVED datetime, 
            AGP_NVALUE numeric(38, 10), 
            AGP_PARAM varchar, 
            AGP_UPDATECOUNT numeric(38, 10), 
            AGP_VALUE varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 