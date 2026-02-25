CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5DWDIMENSIONFIELDS(
                "DMF_BOTNUMBER" numeric(38, 10), 
                "DMF_DESC" varchar, 
                "DMF_DIMTABLE" varchar, 
                "DMF_FIELD" varchar, 
                "DMF_FIELDID" numeric(38, 10), 
                "DMF_LASTSAVED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 