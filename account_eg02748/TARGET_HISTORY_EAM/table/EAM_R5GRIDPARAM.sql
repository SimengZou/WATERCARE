CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5GRIDPARAM_DELETED(
            GDP_DATATYPE varchar, 
            GDP_GRIDID numeric(38, 10), 
            GDP_LASTSAVED datetime, 
            GDP_PARAM varchar, 
            GDP_TAGNAME varchar, 
            GDP_UPDATECOUNT numeric(38, 10), 
            GDP_UPDATED datetime, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 