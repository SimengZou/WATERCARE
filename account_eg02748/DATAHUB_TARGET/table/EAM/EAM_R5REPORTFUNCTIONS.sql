CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5REPORTFUNCTIONS(
                "RFN_FIELDORDER" varchar, 
                "RFN_FROMCLAUSE" varchar, 
                "RFN_FUNCTION" varchar, 
                "RFN_GROUPBY" varchar, 
                "RFN_LASTSAVED" datetime, 
                "RFN_ORDERBY" varchar, 
                "RFN_ORDERTYPE" varchar, 
                "RFN_UPDATECOUNT" numeric(38, 10), 
                "RFN_UPDATED" datetime, 
                "RFN_WHERECLAUSE1" varchar, 
                "RFN_WHERECLAUSE2" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 