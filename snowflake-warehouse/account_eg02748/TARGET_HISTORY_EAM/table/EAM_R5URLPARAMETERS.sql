CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5URLPARAMETERS_DELETED(
            URL_ACTIVE varchar, 
            URL_ALTERNATEPARAMETERNAME varchar, 
            URL_LASTSAVED datetime, 
            URL_PARAMETERNAME varchar, 
            URL_PARAMETERVALUE varchar, 
            URL_SYSTEM varchar, 
            URL_TAB varchar, 
            URL_UPDATECOUNT numeric(38, 10), 
            URL_USEFIELDVALUE varchar, 
            URL_USERFUNCTION varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 