CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5OPSYNCBLOCK(
                "OPB_BLOCKID" numeric(38, 10), 
                "OPB_LASTSAVED" datetime, 
                "OPB_STARTVAL" numeric(38, 10), 
                "OPB_SYNCTABLE" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 