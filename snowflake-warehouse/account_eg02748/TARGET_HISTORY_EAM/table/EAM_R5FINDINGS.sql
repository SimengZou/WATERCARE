CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5FINDINGS_DELETED(
            FND_CLASS varchar, 
            FND_CLASS_ORG varchar, 
            FND_CODE varchar, 
            FND_CREATED datetime, 
            FND_DESC varchar, 
            FND_GEN varchar, 
            FND_LASTSAVED datetime, 
            FND_UPDATECOUNT numeric(38, 10), 
            FND_UPDATED datetime, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 