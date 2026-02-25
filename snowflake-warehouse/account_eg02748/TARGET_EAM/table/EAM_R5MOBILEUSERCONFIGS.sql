CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5MOBILEUSERCONFIGS(
            MUC_CODE varchar, 
            MUC_COMPTYPE varchar, 
            MUC_CREATED datetime, 
            MUC_DESK varchar, 
            MUC_GROUP varchar, 
            MUC_LASTSAVED datetime, 
            MUC_RCODE varchar, 
            MUC_UPDATECOUNT numeric(38, 10), 
            MUC_UPDATED datetime, 
            MUC_USER varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 