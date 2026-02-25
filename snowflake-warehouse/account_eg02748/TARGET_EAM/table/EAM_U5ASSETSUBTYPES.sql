CREATE OR REPLACE TABLE TARGET_EAM.EAM_U5ASSETSUBTYPES(
            AST_ASSETSUBTYPE varchar, 
            AST_ASSETTYPE varchar, 
            CREATED datetime, 
            CREATEDBY varchar, 
            DESCRIPTION varchar, 
            LASTSAVED datetime, 
            UPDATECOUNT numeric(38, 10), 
            UPDATED datetime, 
            UPDATEDBY varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 