CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5PPMPLAN_DELETED(
            PMP_CLASS varchar, 
            PMP_CLASS_ORG varchar, 
            PMP_CODE varchar, 
            PMP_DESC varchar, 
            PMP_LASTSAVED datetime, 
            PMP_OBJECTCLASS varchar, 
            PMP_OBJECTCLASS_ORG varchar, 
            PMP_ORG varchar, 
            PMP_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 