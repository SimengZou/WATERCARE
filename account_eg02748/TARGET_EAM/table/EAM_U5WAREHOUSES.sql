CREATE OR REPLACE TABLE TARGET_EAM.EAM_U5WAREHOUSES(
            CREATED datetime, 
            CREATEDBY varchar, 
            LASTSAVED datetime, 
            UPDATECOUNT numeric(38, 10), 
            UPDATED datetime, 
            UPDATEDBY varchar, 
            WRH_ACCOUNTINGENTITY varchar, 
            WRH_CODE varchar, 
            WRH_DESC varchar, 
            WRH_ENTERPRISELOCATION varchar, 
            WRH_LOCATIONS varchar, 
            WRH_TRANS varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 