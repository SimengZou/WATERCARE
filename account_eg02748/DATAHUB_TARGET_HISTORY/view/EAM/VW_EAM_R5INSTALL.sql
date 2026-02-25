
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5INSTALL
                   as                       
                    SELECT
                        t.INS_CODE,
                        t.INS_DESC,
                        t.INS_COMMENT,
                        t.INS_FIXED,
                        t.INS_MODULE,
                        t.INS_UPDATECOUNT,
                        t.INS_EXTENDED,
                        t.INS_EDCOMMENT,
                        t.INS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5INSTALL as  t
					 union
					        SELECT
                        th.INS_CODE,
                        th.INS_DESC,
                        th.INS_COMMENT,
                        th.INS_FIXED,
                        th.INS_MODULE,
                        th.INS_UPDATECOUNT,
                        th.INS_EXTENDED,
                        th.INS_EDCOMMENT,
                        th.INS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5INSTALL as  th ;
                     