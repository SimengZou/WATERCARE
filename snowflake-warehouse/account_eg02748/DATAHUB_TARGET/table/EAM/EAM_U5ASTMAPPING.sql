CREATE OR REPLACE TABLE TARGET_EAM.EAM_U5ASTMAPPING(
                "AST_ATTRBDESC" varchar, 
                "AST_ATTRIBUTE" varchar, 
                "AST_CLASS" varchar, 
                "AST_CLASSDESC" varchar, 
                "AST_ID" varchar, 
                "AUTOID" numeric(38, 10), 
                "CREATED" datetime, 
                "CREATEDBY" varchar, 
                "LASTSAVED" datetime, 
                "UPDATECOUNT" numeric(38, 10), 
                "UPDATED" datetime, 
                "UPDATEDBY" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 