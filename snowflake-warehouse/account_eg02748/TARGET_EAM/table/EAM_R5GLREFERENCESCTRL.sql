CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5GLREFERENCESCTRL(
            GRC_DATATYPE varchar, 
            GRC_DISPLAYNAME varchar, 
            GRC_DISPLAYSEQ numeric(38, 10), 
            GRC_LASTSAVED datetime, 
            GRC_LENGTH numeric(38, 10), 
            GRC_R5COLUMN varchar, 
            GRC_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 