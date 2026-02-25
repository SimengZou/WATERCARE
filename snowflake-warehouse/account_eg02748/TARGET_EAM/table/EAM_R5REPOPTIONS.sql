CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5REPOPTIONS(
            ROP_FUNCTION varchar, 
            ROP_LASTSAVED datetime, 
            ROP_MEKEY varchar, 
            ROP_PARAMETER varchar, 
            ROP_SEQNO numeric(38, 10), 
            ROP_UPDATECOUNT numeric(38, 10), 
            ROP_UPDATED datetime, 
            ROP_VALUE varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 