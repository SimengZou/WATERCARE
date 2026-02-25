CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5DDFIELD(
            DDF_DATATYPE varchar, 
            DDF_DESC varchar, 
            DDF_FIELDID numeric(38, 10), 
            DDF_LASTSAVED datetime, 
            DDF_LVGRID varchar, 
            DDF_RENTITY varchar, 
            DDF_SOURCENAME varchar, 
            DDF_TABLENAME varchar, 
            DDF_UPDATECOUNT numeric(38, 10), 
            DDF_VALUEMAPID numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 