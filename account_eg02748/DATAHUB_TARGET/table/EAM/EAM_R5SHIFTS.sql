CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5SHIFTS(
                "SHF_CLASS" varchar, 
                "SHF_CLASS_ORG" varchar, 
                "SHF_CODE" varchar, 
                "SHF_DESC" varchar, 
                "SHF_LASTSAVED" datetime, 
                "SHF_LENGTH" numeric(38, 10), 
                "SHF_ORG" varchar, 
                "SHF_START" datetime, 
                "SHF_UPDATECOUNT" numeric(38, 10), 
                "SHF_UPDATED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 