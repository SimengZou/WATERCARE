CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5QUERYFIELD(
                "DQF_COLUMNORDER" numeric(38, 10), 
                "DQF_COLUMNWIDTH" varchar, 
                "DQF_DDSPYID" numeric(38, 10), 
                "DQF_FIELDID" numeric(38, 10), 
                "DQF_LASTSAVED" datetime, 
                "DQF_UPDATECOUNT" numeric(38, 10), 
                "DQF_UPDATED" datetime, 
                "DQF_VIEWTYPE" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 