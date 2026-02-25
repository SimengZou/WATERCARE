CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5HYPERLINK(
            HYP_DATASPY numeric(38, 10), 
            HYP_DESTELEMENTID varchar, 
            HYP_DESTPAGENAME varchar, 
            HYP_LASTSAVED datetime, 
            HYP_LINKNAME varchar, 
            HYP_PERFORMEXACTQUERY varchar, 
            HYP_PK numeric(38, 10), 
            HYP_SCREENMODE varchar, 
            HYP_SOURCEELEMENTID varchar, 
            HYP_SOURCEPAGENAME varchar, 
            HYP_SRCLINENUMBER numeric(38, 10), 
            HYP_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 