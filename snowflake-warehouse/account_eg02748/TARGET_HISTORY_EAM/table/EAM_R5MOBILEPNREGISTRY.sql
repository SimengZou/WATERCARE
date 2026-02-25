CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5MOBILEPNREGISTRY_DELETED(
            MPN_APPID varchar, 
            MPN_CREATED datetime, 
            MPN_CREATEDBY varchar, 
            MPN_DEVICEID varchar, 
            MPN_LASTSAVED datetime, 
            MPN_PLATFORM varchar, 
            MPN_TOKEN varchar, 
            MPN_UPDATECOUNT numeric(38, 10), 
            MPN_UPDATED datetime, 
            MPN_UPDATEDBY varchar, 
            MPN_USER varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 