CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_U5SPOTCODES_DELETED(
            CREATED datetime, 
            CREATEDBY varchar, 
            DESCRIPTION varchar, 
            LASTSAVED datetime, 
            SPC_CODE varchar, 
            SPC_DESC varchar, 
            SPC_NOTUSED varchar, 
            SPC_TYPE varchar, 
            UPDATECOUNT numeric(38, 10), 
            UPDATED datetime, 
            UPDATEDBY varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 