CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5REPORTFIELDS_DELETED(
            RFL_BOTNUMBER numeric(38, 10), 
            RFL_DATATYPE varchar, 
            RFL_FIELD varchar, 
            RFL_FUNCTION varchar, 
            RFL_LASTSAVED datetime, 
            RFL_LINE numeric(38, 10), 
            RFL_SHOWFIELD varchar, 
            RFL_UPDATECOUNT numeric(38, 10), 
            RFL_UPDATED datetime, 
            RFL_WIDTH numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 