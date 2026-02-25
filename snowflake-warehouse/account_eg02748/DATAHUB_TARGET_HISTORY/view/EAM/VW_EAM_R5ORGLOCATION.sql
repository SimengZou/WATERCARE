
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ORGLOCATION
                   as                       
                    SELECT
                        t.OGL_ORG,
                        t.OGL_BODGROUP,
                        t.OGL_ENTERPRISELOCATION,
                        t.OGL_UPDATECOUNT,
                        t.OGL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ORGLOCATION as  t
					 union
					        SELECT
                        th.OGL_ORG,
                        th.OGL_BODGROUP,
                        th.OGL_ENTERPRISELOCATION,
                        th.OGL_UPDATECOUNT,
                        th.OGL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ORGLOCATION as  th ;
                     