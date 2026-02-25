CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5UNAVAILABLEGRIDFIELDS(
            UGF_FIELDID varchar, 
            UGF_GRIDNAME varchar, 
            UGF_LASTSAVED datetime, 
            UGF_MEKEY varchar, 
            UGF_USERGROUP varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 