CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5EDITIONUCODES(
                "EDU_CODE" varchar, 
                "EDU_DESC" varchar, 
                "EDU_LANG" varchar, 
                "EDU_LASTSAVED" datetime, 
                "EDU_MARKET" varchar, 
                "EDU_RCODE" varchar, 
                "EDU_RENTITY" varchar, 
                "EDU_TEXTCODE" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 