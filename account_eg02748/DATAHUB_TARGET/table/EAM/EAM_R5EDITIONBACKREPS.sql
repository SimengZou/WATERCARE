CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5EDITIONBACKREPS(
                "EBR_FUNCTION" varchar, 
                "EBR_LASTSAVED" datetime, 
                "EBR_MEFLAG" varchar, 
                "EBR_REPORT" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 