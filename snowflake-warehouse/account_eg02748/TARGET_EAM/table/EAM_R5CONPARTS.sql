CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5CONPARTS(
            CPA_CONTRACT varchar, 
            CPA_LASTSAVED datetime, 
            CPA_LEADTIME numeric(38, 10), 
            CPA_MULTIPLY numeric(38, 10), 
            CPA_PART varchar, 
            CPA_PART_ORG varchar, 
            CPA_PRICE numeric(38, 10), 
            CPA_PURUOM varchar, 
            CPA_REF varchar, 
            CPA_SUPPLIER varchar, 
            CPA_SUPPLIER_ORG varchar, 
            CPA_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 