CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5PMFORECASTEQUIPLIST(
            PFL_LASTSAVED datetime, 
            PFL_OBJECT varchar, 
            PFL_OBJECT_ORG varchar, 
            PFL_PARENT varchar, 
            PFL_PARENT_ORG varchar, 
            PFL_SELECT varchar, 
            PFL_SESSIONID numeric(38, 10), 
            PFL_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 