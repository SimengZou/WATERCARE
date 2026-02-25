CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5ALERTIONPULSE(
            ALI_ALERT varchar, 
            ALI_DELAY numeric(38, 10), 
            ALI_DELAYUOM varchar, 
            ALI_DESCRIPTIONFIELDID numeric(38, 10), 
            ALI_LASTSAVED datetime, 
            ALI_RECIPIENTFIELDID numeric(38, 10), 
            ALI_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 