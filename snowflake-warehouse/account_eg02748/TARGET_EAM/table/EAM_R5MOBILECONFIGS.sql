CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5MOBILECONFIGS(
            MCF_CODE varchar, 
            MCF_COMPTYPE varchar, 
            MCF_CONFIG numeric(38, 10), 
            MCF_CREATED datetime, 
            MCF_DESK varchar, 
            MCF_GROUP varchar, 
            MCF_LASTSAVED datetime, 
            MCF_RCODE varchar, 
            MCF_UPDATECOUNT numeric(38, 10), 
            MCF_UPDATED datetime, 
            MCF_USER varchar, 
            MCF_XMLCONFIG varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 