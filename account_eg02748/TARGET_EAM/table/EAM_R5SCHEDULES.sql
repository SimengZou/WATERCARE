CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5SCHEDULES(
            SCH_CODE varchar, 
            SCH_DAYOFMONTH varchar, 
            SCH_DAYOFWEEK varchar, 
            SCH_DESCRIPTION varchar, 
            SCH_HOUR varchar, 
            SCH_LASTSAVED datetime, 
            SCH_MINUTE varchar, 
            SCH_MONTH varchar, 
            SCH_NAME varchar, 
            SCH_TYPE varchar, 
            SCH_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 