CREATE OR REPLACE TABLE DATAHUB_TARGET.EAM_R5EDITIONUCODES(
                        EDU_RCODE varchar(4),
                        EDU_CODE varchar(4),
                        EDU_LANG varchar(2),
                        EDU_DESC varchar(80),
                        EDU_RENTITY varchar(4),
                        EDU_MARKET varchar(8),
                        EDU_TEXTCODE varchar(12),
                        EDU_LASTSAVED datetime(0)
                        ,etl_load_datetime timestamp,
                          etl_load_metadatafilename varchar(255),
                           ETL_LASTSAVED datetime(0),
			ETL_DELETED boolean
                    );