
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FUNNOPERM
                   as                       
                    SELECT
                        t.FPN_FUNCTION,
                        t.FPN_NOSELECT,
                        t.FPN_NOINSERT,
                        t.FPN_NODELETE,
                        t.FPN_NOUPDATE,
                        t.FPN_UPDATECOUNT,
                        t.FPN_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FUNNOPERM as  t
					 union
					        SELECT
                        th.FPN_FUNCTION,
                        th.FPN_NOSELECT,
                        th.FPN_NOINSERT,
                        th.FPN_NODELETE,
                        th.FPN_NOUPDATE,
                        th.FPN_UPDATECOUNT,
                        th.FPN_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FUNNOPERM as  th ;
                     