
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5USERORGANIZATION
                   as                       
                    SELECT
                        t.UOG_USER,
                        t.UOG_ORG,
                        t.UOG_DEFAULT,
                        t.UOG_COMMON,
                        t.UOG_GROUP,
                        t.UOG_REQAPPVLIMIT,
                        t.UOG_REQAUTHAPPVLIMIT,
                        t.UOG_PORDAPPVLIMIT,
                        t.UOG_PORDAUTHAPPVLIMIT,
                        t.UOG_PICAPPVLIMIT,
                        t.UOG_INVAPPVLIMIT,
                        t.UOG_INVAPPVLIMITNONPO,
                        t.UOG_UPDATECOUNT,
                        t.UOG_CREATED,
                        t.UOG_UPDATED,
                        t.UOG_LASTSAVED,
                        t.UOG_ROLE,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5USERORGANIZATION as  t
					 union
					        SELECT
                        th.UOG_USER,
                        th.UOG_ORG,
                        th.UOG_DEFAULT,
                        th.UOG_COMMON,
                        th.UOG_GROUP,
                        th.UOG_REQAPPVLIMIT,
                        th.UOG_REQAUTHAPPVLIMIT,
                        th.UOG_PORDAPPVLIMIT,
                        th.UOG_PORDAUTHAPPVLIMIT,
                        th.UOG_PICAPPVLIMIT,
                        th.UOG_INVAPPVLIMIT,
                        th.UOG_INVAPPVLIMITNONPO,
                        th.UOG_UPDATECOUNT,
                        th.UOG_CREATED,
                        th.UOG_UPDATED,
                        th.UOG_LASTSAVED,
                        th.UOG_ROLE,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5USERORGANIZATION as  th ;
                     