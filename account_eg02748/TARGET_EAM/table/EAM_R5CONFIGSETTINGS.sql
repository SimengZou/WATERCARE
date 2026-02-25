CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5CONFIGSETTINGS(
            CFS_CODE varchar, 
            CFS_COMPTYPE varchar, 
            CFS_CONFIG numeric(38, 10), 
            CFS_CREATED datetime, 
            CFS_DESK varchar, 
            CFS_GROUP varchar, 
            CFS_LASTSAVED datetime, 
            CFS_UPDATECOUNT numeric(38, 10), 
            CFS_UPDATED datetime, 
            CFS_USER varchar, 
            CFS_XMLCONFIG varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 