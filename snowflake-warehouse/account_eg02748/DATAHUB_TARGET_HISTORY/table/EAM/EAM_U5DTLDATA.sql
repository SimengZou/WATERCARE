CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_U5DTLDATA_DELETED(
                "COLUMNMAXLENGTH" numeric(38, 10), 
                "COLUMNNAME" varchar, 
                "COLUMNNULLABLE" varchar, 
                "COLUMNPRECISION" numeric(38, 10), 
                "COLUMNSCALING" numeric(38, 10), 
                "COLUMNTYPE" varchar, 
                "LASTSAVED" datetime, 
                "TABLENAME" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 