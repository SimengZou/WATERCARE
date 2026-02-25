
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5TABNOPERM
                   as                       
                    SELECT
                        t.TPN_FUNCTION,
                        t.TPN_TAB,
                        t.TPN_NOSELECT,
                        t.TPN_NOINSERT,
                        t.TPN_NODELETE,
                        t.TPN_NOUPDATE,
                        t.TPN_UPDATECOUNT,
                        t.TPN_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5TABNOPERM as  t
					 union
					        SELECT
                        th.TPN_FUNCTION,
                        th.TPN_TAB,
                        th.TPN_NOSELECT,
                        th.TPN_NOINSERT,
                        th.TPN_NODELETE,
                        th.TPN_NOUPDATE,
                        th.TPN_UPDATECOUNT,
                        th.TPN_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5TABNOPERM as  th ;
                     