CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5MOBILEQUERIES(
                "MQR_COLUMNMAP" varchar, 
                "MQR_GRIDNAME" varchar, 
                "MQR_LASTSAVED" datetime, 
                "MQR_PREPAREGRID" varchar, 
                "MQR_PREPARETABLEUSED" varchar, 
                "MQR_SINGLETHREADPERCONN" varchar, 
                "MQR_TABLENAME" varchar, 
                "MQR_UPDATECOUNT" numeric(38, 10), 
                "MQR_UPDATED" datetime, 
                "MQR_VERSION" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 