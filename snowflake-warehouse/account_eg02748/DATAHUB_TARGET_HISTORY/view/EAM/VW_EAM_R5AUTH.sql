
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5AUTH
                   as                       
                    SELECT
                        t.AUT_GROUP,
                        t.AUT_USER,
                        t.AUT_ENTITY,
                        t.AUT_RENTITY,
                        t.AUT_STATUS,
                        t.AUT_STATNEW,
                        t.AUT_TYPE,
                        t.AUT_UPDATECOUNT,
                        t.AUT_CREATED,
                        t.AUT_UPDATED,
                        t.AUT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5AUTH as  t
					 union
					        SELECT
                        th.AUT_GROUP,
                        th.AUT_USER,
                        th.AUT_ENTITY,
                        th.AUT_RENTITY,
                        th.AUT_STATUS,
                        th.AUT_STATNEW,
                        th.AUT_TYPE,
                        th.AUT_UPDATECOUNT,
                        th.AUT_CREATED,
                        th.AUT_UPDATED,
                        th.AUT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5AUTH as  th ;
                     