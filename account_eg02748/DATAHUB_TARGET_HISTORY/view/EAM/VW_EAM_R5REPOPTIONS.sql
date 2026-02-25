
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5REPOPTIONS
                   as                       
                    SELECT
                        t.ROP_FUNCTION,
                        t.ROP_PARAMETER,
                        t.ROP_SEQNO,
                        t.ROP_VALUE,
                        t.ROP_UPDATECOUNT,
                        t.ROP_UPDATED,
                        t.ROP_MEKEY,
                        t.ROP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5REPOPTIONS as  t
					 union
					        SELECT
                        th.ROP_FUNCTION,
                        th.ROP_PARAMETER,
                        th.ROP_SEQNO,
                        th.ROP_VALUE,
                        th.ROP_UPDATECOUNT,
                        th.ROP_UPDATED,
                        th.ROP_MEKEY,
                        th.ROP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5REPOPTIONS as  th ;
                     