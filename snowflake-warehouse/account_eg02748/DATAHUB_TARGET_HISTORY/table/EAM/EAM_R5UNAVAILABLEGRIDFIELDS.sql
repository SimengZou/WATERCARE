CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5UNAVAILABLEGRIDFIELDS_DELETED(
                "UGF_FIELDID" varchar, 
                "UGF_GRIDNAME" varchar, 
                "UGF_LASTSAVED" datetime, 
                "UGF_MEKEY" varchar, 
                "UGF_USERGROUP" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 