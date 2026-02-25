CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5SCHEDGROUPS_DELETED(
            SCG_CLASS varchar, 
            SCG_CLASS_ORG varchar, 
            SCG_CODE varchar, 
            SCG_DESC varchar, 
            SCG_LASTSAVED datetime, 
            SCG_NOTUSED varchar, 
            SCG_ORG varchar, 
            SCG_PROFILEPICTURE varchar, 
            SCG_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 