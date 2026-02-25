CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5USERTABCOLUMNS_DELETED(
                "UTC_COLLATION" varchar, 
                "UTC_COLUMNNAME" varchar, 
                "UTC_COL_XTYPE" numeric(38, 10), 
                "UTC_DATABASE" varchar, 
                "UTC_DATATYPE" varchar, 
                "UTC_ISID" varchar, 
                "UTC_LASTSAVED" datetime, 
                "UTC_OBJ_XTYPE" varchar, 
                "UTC_TABLENAME" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 