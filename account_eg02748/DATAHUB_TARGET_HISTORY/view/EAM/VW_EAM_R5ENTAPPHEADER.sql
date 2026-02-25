
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ENTAPPHEADER
                   as                       
                    SELECT
                        t.EAH_PK,
                        t.EAH_RENTITY,
                        t.EAH_ENTITY,
                        t.EAH_CODE,
                        t.EAH_REVISION,
                        t.EAH_APPLIST,
                        t.EAH_APPDATE,
                        t.EAH_USER,
                        t.EAH_DATE,
                        t.EAH_REASON,
                        t.EAH_UPDATECOUNT,
                        t.EAH_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ENTAPPHEADER as  t
					 union
					        SELECT
                        th.EAH_PK,
                        th.EAH_RENTITY,
                        th.EAH_ENTITY,
                        th.EAH_CODE,
                        th.EAH_REVISION,
                        th.EAH_APPLIST,
                        th.EAH_APPDATE,
                        th.EAH_USER,
                        th.EAH_DATE,
                        th.EAH_REASON,
                        th.EAH_UPDATECOUNT,
                        th.EAH_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ENTAPPHEADER as  th ;
                     