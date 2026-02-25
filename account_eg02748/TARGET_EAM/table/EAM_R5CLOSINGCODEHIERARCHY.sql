CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5CLOSINGCODEHIERARCHY(
            CCH_CHILDCLOSINGCODE varchar, 
            CCH_CHILDCLOSINGCODETYPE varchar, 
            CCH_LASTSAVED datetime, 
            CCH_OBJECT varchar, 
            CCH_OBJECT_ORG varchar, 
            CCH_PARENTCLOSINGCODE varchar, 
            CCH_PARENTCLOSINGCODETYPE varchar, 
            CCH_REPLDEPT varchar, 
            CCH_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 