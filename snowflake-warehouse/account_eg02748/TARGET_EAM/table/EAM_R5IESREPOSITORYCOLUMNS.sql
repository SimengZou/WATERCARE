CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5IESREPOSITORYCOLUMNS(
            ENC_ALIAS varchar, 
            ENC_COLUMNNAME varchar, 
            ENC_DATATYPE varchar, 
            ENC_DISPLAYORDER numeric(38, 10), 
            ENC_FACET varchar, 
            ENC_FIELDNAME varchar, 
            ENC_HIDDENFROMSEARCHRESULTS varchar, 
            ENC_ISJSPKEYFIELD varchar, 
            ENC_LASTSAVED datetime, 
            ENC_PKORDER numeric(38, 10), 
            ENC_REPOSITORYID varchar, 
            ENC_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 