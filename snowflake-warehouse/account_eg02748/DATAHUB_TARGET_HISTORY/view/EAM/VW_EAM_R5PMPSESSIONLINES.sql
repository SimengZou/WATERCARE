
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PMPSESSIONLINES
                   as                       
                    SELECT
                        t.PPL_LINE,
                        t.PPL_OBJECT,
                        t.PPL_OBJORG,
                        t.PPL_PPM,
                        t.PPL_NESTINGREF,
                        t.PPL_SESSIONID,
                        t.PPL_UPDATECOUNT,
                        t.PPL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PMPSESSIONLINES as  t
					 union
					        SELECT
                        th.PPL_LINE,
                        th.PPL_OBJECT,
                        th.PPL_OBJORG,
                        th.PPL_PPM,
                        th.PPL_NESTINGREF,
                        th.PPL_SESSIONID,
                        th.PPL_UPDATECOUNT,
                        th.PPL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PMPSESSIONLINES as  th ;
                     