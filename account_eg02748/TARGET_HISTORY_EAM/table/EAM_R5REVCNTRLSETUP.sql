CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5REVCNTRLSETUP_DELETED(
            RCS_ELEMENTID varchar, 
            RCS_LASTSAVED datetime, 
            RCS_PAGENAME varchar, 
            RCS_PROTECTED varchar, 
            RCS_UPDATECOUNT numeric(38, 10), 
            RCS_XPATH varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 