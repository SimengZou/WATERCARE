CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5REPORTSUBTOTAL(
                "RST_BOTNUMBER" numeric(38, 10), 
                "RST_DATATYPE" varchar, 
                "RST_FIELD" varchar, 
                "RST_FUNCTION" varchar, 
                "RST_GROUPLINE" numeric(38, 10), 
                "RST_LASTSAVED" datetime, 
                "RST_LINE" numeric(38, 10), 
                "RST_UPDATECOUNT" numeric(38, 10), 
                "RST_UPDATED" datetime, 
                "RST_WIDTH" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 