CREATE OR REPLACE TABLE TARGET_EAM.EAM_U5MYFACILITIES(
                "CREATED" datetime, 
                "CREATEDBY" varchar, 
                "LASTSAVED" datetime, 
                "MFA_FACCODE" varchar, 
                "MFA_FACDESC" varchar, 
                "MFA_FACORG" varchar, 
                "MFA_MRC" varchar, 
                "MFA_MRCDESC" varchar, 
                "MFA_USER" varchar, 
                "UPDATECOUNT" numeric(38, 10), 
                "UPDATED" datetime, 
                "UPDATEDBY" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 