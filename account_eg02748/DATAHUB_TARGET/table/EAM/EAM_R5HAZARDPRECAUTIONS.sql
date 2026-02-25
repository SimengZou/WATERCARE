CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5HAZARDPRECAUTIONS(
                "HZP_HAZARD" varchar, 
                "HZP_HAZARD_ORG" varchar, 
                "HZP_LASTSAVED" datetime, 
                "HZP_PRECAUTION" varchar, 
                "HZP_PRECAUTION_ORG" varchar, 
                "HZP_REVISION" numeric(38, 10), 
                "HZP_SEQUENCE" numeric(38, 10), 
                "HZP_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 