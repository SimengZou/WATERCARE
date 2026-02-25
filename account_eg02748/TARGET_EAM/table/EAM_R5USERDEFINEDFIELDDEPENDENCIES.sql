CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5USERDEFINEDFIELDDEPENDENCIES(
            UFD_GRIDCACHE varchar, 
            UFD_LASTSAVED datetime, 
            UFD_LAYOUTCACHE varchar, 
            UFD_PAGENAME varchar, 
            UFD_RENTITY varchar, 
            UFD_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 