
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5PUBLICHOLIDAYS
                   as                       
                    SELECT
                        t.AUTO,
                        t.HLY_ID,
                        t.HLY_ORG,
                        t.HLY_CONCODE,
                        t.HLY_YEAR,
                        t.HLY_DATE,
                        t.HLY_NAME,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.LASTSAVED,
                        t.UPDATECOUNT,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5PUBLICHOLIDAYS as  t
					 union
					        SELECT
                        th.AUTO,
                        th.HLY_ID,
                        th.HLY_ORG,
                        th.HLY_CONCODE,
                        th.HLY_YEAR,
                        th.HLY_DATE,
                        th.HLY_NAME,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.LASTSAVED,
                        th.UPDATECOUNT,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5PUBLICHOLIDAYS as  th ;
                     